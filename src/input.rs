use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use memchr::{memchr, memchr_iter};
use rayon::prelude::*;

const CHUNK_BYTES: u64 = 4 * 1024 * 1024;

pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut row = [&[][..]; N];
    let mut rest = line;
    for (index, value) in row.iter_mut().enumerate() {
        let comma = memchr(b',', rest);
        if comma.is_none() != (index + 1 == N) {
            return None;
        }
        let mut field = &rest[..comma.unwrap_or(rest.len())];
        if field.first() == Some(&b'"') {
            if field.len() < 2 || field.last() != Some(&b'"') {
                return None;
            }
            field = &field[1..field.len() - 1];
        }
        if memchr(b'"', field).is_some() {
            return None;
        }
        *value = field;
        if let Some(comma) = comma {
            rest = &rest[comma + 1..];
        }
    }
    Some(row)
}

pub fn csv_files(directory: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in directory.read_dir()? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "csv") && path.metadata()?.is_file() {
            paths.push(path);
        }
    }
    Ok(paths)
}

pub fn read_file(path: &Path, mut row: impl FnMut(&[u8])) -> io::Result<()> {
    let mut input = BufReader::new(File::open(path)?);
    let mut line = Vec::new();
    input.read_until(b'\n', &mut line)?;
    loop {
        line.clear();
        if input.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        row(line.strip_suffix(b"\n").unwrap_or(&line));
    }
}

struct Range<'a> {
    path: &'a Path,
    start: u64,
    end: u64,
    tail: bool,
}

fn scan_range(
    range: &Range<'_>,
    buffer: &mut Vec<u8>,
    mut row: impl FnMut(&[u8]),
) -> io::Result<()> {
    let mut file = File::open(range.path)?;
    // Read one preceding byte to distinguish a record boundary from a partial record.
    let read_start = range.start.saturating_sub(1);
    file.seek(SeekFrom::Start(read_start))?;
    buffer.resize((range.end - read_start) as usize, 0);
    file.read_exact(buffer)?;
    let Some(header_or_partial_end) = memchr(b'\n', buffer) else {
        return Ok(());
    };
    if !range.tail && buffer.last() != Some(&b'\n') {
        BufReader::new(file).read_until(b'\n', buffer)?;
    }
    let data = &buffer[header_or_partial_end + 1..];
    let mut start = 0;
    for end in memchr_iter(b'\n', data) {
        row(&data[start..end]);
        start = end + 1;
    }
    if start < data.len() {
        row(&data[start..]);
    }
    Ok(())
}

pub fn scan<T: Send>(
    paths: &[PathBuf],
    workers: usize,
    start_worker: impl Fn() -> T + Sync,
    visit: impl Fn(&mut T, &[u8]) + Sync,
) -> io::Result<Vec<T>> {
    let mut files = paths
        .iter()
        .map(|path| Ok((path, path.metadata()?.len())))
        .collect::<io::Result<Vec<_>>>()?;
    files.sort_unstable_by_key(|&(_, size)| std::cmp::Reverse(size));
    let mut ranges = Vec::new();
    for (path, size) in files {
        for start in (0..size).step_by(CHUNK_BYTES as usize) {
            let end = (start + CHUNK_BYTES).min(size);
            ranges.push(Range {
                path,
                start,
                end,
                tail: end == size,
            });
        }
    }
    let next = AtomicUsize::new(0);
    (0..workers)
        .into_par_iter()
        .map(|_| {
            let mut state = start_worker();
            let mut buffer = Vec::new();
            while let Some(range) = ranges.get(next.fetch_add(1, Ordering::Relaxed)) {
                scan_range(range, &mut buffer, |line| visit(&mut state, line))?;
            }
            Ok(state)
        })
        .collect()
}
