mod attribution;
mod events;
mod input;
mod model;
mod output;
mod sessions;

use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use rayon::prelude::*;

use events::Catalog;
use model::{Keys, Window, timestamp};
use sessions::Timeline;

fn finished(stage: &str, previous: &mut Instant) {
    eprintln!("{stage}: {:.3}s", previous.elapsed().as_secs_f64());
    *previous = Instant::now();
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut previous = Instant::now();
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let output = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let workers = env::var("NCPU")
        .map_or(Ok(8), |value| value.parse::<usize>())?
        .max(1);
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build_global()?;

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json"))?)?;
    let bound = |key: &str| {
        manifest[key]
            .as_str()
            .and_then(|value| {
                let bytes = value.as_bytes();
                if bytes.len() == 20 && bytes[19] == b'Z' {
                    timestamp(bytes)
                } else {
                    None
                }
            })
            .ok_or_else(|| format!("invalid manifest {key}"))
    };
    let window = Window {
        start: bound("window_start")?,
        end: bound("window_end")?,
    };
    if window.start >= window.end {
        return Err("manifest window is empty or reversed".into());
    }
    let keys = Keys::default();
    let catalog = Catalog::load(&data, &keys)?;
    let (aliases, copies) = rayon::join(
        || events::load_aliases(&data, &keys),
        || events::read_copies(&data, &keys, window, &catalog, workers),
    );
    let aliases = aliases?;
    let copies = copies?;
    finished("load identity links and parse events", &mut previous);

    let survivors = events::survivors(copies, &aliases);
    finished("deduplicate and route events", &mut previous);
    let timelines: Vec<Timeline> = survivors
        .into_par_iter()
        .map(|parts| sessions::build(parts, window))
        .collect();
    finished("build sessions", &mut previous);
    let campaigns = attribution::aggregate(
        &data, &keys, window, &catalog, &aliases, &timelines, workers,
    )?;
    finished("parse clicks, attribute and count assists", &mut previous);
    output::write(&output, &catalog, &keys, &timelines, &campaigns)?;
    finished("write Parquet", &mut previous);
    Ok(())
}
