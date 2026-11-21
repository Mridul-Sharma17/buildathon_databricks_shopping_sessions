use std::cmp::Ordering;
use std::hash::BuildHasher;
use std::sync::{Arc, RwLock};

use rustc_hash::{FxBuildHasher, FxHashMap};

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const GAP: i64 = 1_800_000;
pub const NO_TIME: i64 = i64::MIN;
pub const NO_CATEGORY: u32 = u32::MAX;

#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(u64, u64);

impl Key {
    pub fn hash(self) -> usize {
        FxBuildHasher.hash_one(self) as usize
    }

    fn long(self) -> bool {
        self.1 & 0xff == 0xff
    }

    fn with_inline<T>(self, f: impl FnOnce(&[u8]) -> T) -> T {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.0.to_be_bytes());
        bytes[8..].copy_from_slice(&self.1.to_be_bytes());
        f(&bytes[..bytes[15] as usize])
    }
}

#[derive(Default)]
struct LongKeys {
    index: FxHashMap<Arc<[u8]>, Key>,
    bytes: Vec<Arc<[u8]>>,
}

#[derive(Default)]
pub struct Keys(RwLock<LongKeys>);

impl Keys {
    pub fn intern(&self, bytes: &[u8]) -> Key {
        if bytes.len() <= 15 {
            let mut packed = [0; 16];
            packed[..bytes.len()].copy_from_slice(bytes);
            packed[15] = bytes.len() as u8;
            return Key(
                u64::from_be_bytes(packed[..8].try_into().unwrap()),
                u64::from_be_bytes(packed[8..].try_into().unwrap()),
            );
        }
        if let Some(&key) = self.0.read().unwrap().index.get(bytes) {
            return key;
        }
        let mut long = self.0.write().unwrap();
        if let Some(&key) = long.index.get(bytes) {
            return key;
        }
        let bytes: Arc<[u8]> = bytes.into();
        let key = Key(
            FxBuildHasher.hash_one(&bytes),
            ((long.bytes.len() as u64) << 8) | 0xff,
        );
        long.bytes.push(bytes.clone());
        long.index.insert(bytes, key);
        key
    }

    pub fn with_bytes<T>(&self, key: Key, f: impl FnOnce(&[u8]) -> T) -> T {
        if key.long() {
            f(&self.0.read().unwrap().bytes[(key.1 >> 8) as usize])
        } else {
            key.with_inline(f)
        }
    }

    pub fn cmp(&self, left: Key, right: Key) -> Ordering {
        if !left.long() && !right.long() {
            return left.cmp(&right);
        }
        let long = self.0.read().unwrap();
        let get = |key: Key| &long.bytes[(key.1 >> 8) as usize];
        match (left.long(), right.long()) {
            (true, true) => get(left).cmp(get(right)),
            (true, false) => right.with_inline(|bytes| get(left).as_ref().cmp(bytes)),
            (false, true) => left.with_inline(|bytes| bytes.cmp(get(right).as_ref())),
            (false, false) => unreachable!(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct Window {
    pub start: i64,
    pub end: i64,
}

impl Window {
    pub fn first_day(self) -> i32 {
        self.start.div_euclid(DAY) as i32
    }

    pub fn days(self) -> usize {
        ((self.end - 1).div_euclid(DAY) - i64::from(self.first_day()) + 1) as usize
    }
}

pub fn unsigned(bytes: &[u8], limit: i64) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0i64;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 || value > (limit - i64::from(digit)) / 10 {
            return None;
        }
        value = value * 10 + i64::from(digit);
    }
    Some(value)
}

fn pair(bytes: &[u8], start: usize) -> Option<i64> {
    let tens = bytes[start].wrapping_sub(b'0');
    let ones = bytes[start + 1].wrapping_sub(b'0');
    (tens <= 9 && ones <= 9).then_some(i64::from(tens) * 10 + i64::from(ones))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let within = year - era * 400;
    let month = if month > 2 { month - 3 } else { month + 9 };
    era * 146_097 + within * 365 + within / 4 - within / 100 + (153 * month + 2) / 5 + day
        - 1
        - 719_468
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if !matches!(bytes.len(), 20 | 25) || bytes[10] != b'T' {
        return unsigned(bytes, i64::MAX);
    }
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year = pair(bytes, 0)? * 100 + pair(bytes, 2)?;
    let month = pair(bytes, 5)?;
    let day = pair(bytes, 8)?;
    let hour = pair(bytes, 11)?;
    let minute = pair(bytes, 14)?;
    let second = pair(bytes, 17)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => 28 + i64::from(leap),
        _ => return None,
    };
    if day == 0 || day > max_day || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let offset = match (bytes.len(), bytes[19]) {
        (20, b'Z') => 0,
        (25, sign @ (b'+' | b'-')) if bytes[22] == b':' => {
            let hours = pair(bytes, 20)?;
            let minutes = pair(bytes, 23)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let offset = hours * 60 + minutes;
            if sign == b'-' { -offset } else { offset }
        }
        _ => return None,
    };
    Some(
        days_from_civil(year, month, day) * DAY
            + ((hour * 60 + minute - offset) * 60 + second) * 1000,
    )
}
