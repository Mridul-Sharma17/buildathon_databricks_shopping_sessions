use std::io;
use std::path::Path;

use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::input::{csv_files, fields, read_file, scan};
use crate::model::{DAY, Key, Keys, NO_CATEGORY, NO_TIME, Window, timestamp, unsigned};

const EVENT_SHARDS: usize = 256;
pub const PERSON_SHARDS: usize = 128;

pub struct Catalog {
    pub products: FxHashMap<Key, u32>,
    pub categories: Vec<Key>,
    pub unknown: u32,
    pub campaigns: FxHashMap<Key, u32>,
    pub campaign_names: Vec<(Key, Key)>,
}

impl Catalog {
    pub fn load(data: &Path, keys: &Keys) -> io::Result<Self> {
        let mut products = Vec::new();
        let unknown_key = keys.intern(b"unknown");
        let mut categories = vec![unknown_key];
        read_file(&data.join("products.csv"), |line| {
            if let Some([id, category]) = fields(line) {
                let category = keys.intern(category);
                categories.push(category);
                products.push((keys.intern(id), category));
            }
        })?;
        categories.sort_unstable_by(|&a, &b| keys.cmp(a, b));
        categories.dedup();
        let category_rank: FxHashMap<_, _> = categories
            .iter()
            .enumerate()
            .map(|(index, &key)| (key, index as u32))
            .collect();
        let products = products
            .into_iter()
            .map(|(id, category)| (id, category_rank[&category]))
            .collect();
        let mut campaigns = FxHashMap::default();
        let organic = keys.intern(b"organic");
        let mut campaign_names = vec![(organic, organic)];
        read_file(&data.join("campaigns.csv"), |line| {
            if let Some([id, channel]) = fields(line) {
                let id = keys.intern(id);
                campaigns.insert(id, campaign_names.len() as u32);
                campaign_names.push((id, keys.intern(channel)));
            }
        })?;
        Ok(Self {
            products,
            unknown: category_rank[&unknown_key],
            categories,
            campaigns,
            campaign_names,
        })
    }
}

fn root(parents: &mut [u32], mut node: u32) -> u32 {
    while node != parents[node as usize] {
        let parent = parents[node as usize];
        parents[node as usize] = parents[parent as usize];
        node = parent;
    }
    node
}

pub fn load_aliases(data: &Path, keys: &Keys) -> io::Result<FxHashMap<Key, Key>> {
    let mut ids = FxHashMap::default();
    let mut parents = Vec::<u32>::new();
    let mut sizes = Vec::<u32>::new();
    for path in csv_files(&data.join("links"))? {
        read_file(&path, |line| {
            let Some([left, right]) = fields(line) else {
                return;
            };
            if left.is_empty() || right.is_empty() {
                return;
            }
            let mut number = |bytes: &[u8]| {
                *ids.entry(keys.intern(bytes)).or_insert_with(|| {
                    let id = parents.len() as u32;
                    parents.push(id);
                    sizes.push(1);
                    id
                })
            };
            let left = number(left);
            let right = number(right);
            let (mut left, mut right) = (root(&mut parents, left), root(&mut parents, right));
            if left == right {
                return;
            }
            if sizes[left as usize] < sizes[right as usize] {
                std::mem::swap(&mut left, &mut right);
            }
            parents[right as usize] = left;
            sizes[left as usize] += sizes[right as usize];
        })?;
    }
    let mut names = vec![None; parents.len()];
    for (&key, &number) in &ids {
        let name = &mut names[root(&mut parents, number) as usize];
        if name.is_none_or(|prior| keys.cmp(key, prior).is_lt()) {
            *name = Some(key);
        }
    }
    Ok(ids
        .into_iter()
        .filter_map(|(key, number)| {
            let canonical = names[root(&mut parents, number) as usize].unwrap();
            (key != canonical).then_some((key, canonical))
        })
        .collect())
}

#[derive(Clone, Copy, Default)]
pub struct Event {
    pub ts: i64,
    pub cents: u32,
    pub category: u32,
}

#[derive(Clone, Copy)]
pub struct CopyRow {
    id: Key,
    user: Key,
    ingest: i64,
    event: Event,
}

pub struct PersonEvent {
    pub person: Key,
    pub event: Event,
}

fn parse_event(line: &[u8], keys: &Keys, window: Window, catalog: &Catalog) -> Option<CopyRow> {
    let [id, user, ts, ingest, kind, product, price] = fields(line)?;
    if id.is_empty() {
        return None;
    }
    let ingest = timestamp(ingest).expect("ingest_ts is valid F1");
    let mut row = CopyRow {
        id: keys.intern(id),
        user: Key::default(),
        ingest,
        event: Event {
            ts: NO_TIME,
            cents: 0,
            category: NO_CATEGORY,
        },
    };
    // A well-formed invalid row still takes part in earliest-copy selection.
    if user.is_empty() || product.is_empty() {
        return Some(row);
    }
    let Some(ts) =
        timestamp(ts).filter(|&ts| ts >= window.start && ts < window.end && ingest - ts <= DAY)
    else {
        return Some(row);
    };
    if kind == b"purchase" {
        let Some(cents) = unsigned(price, 10_000_000).filter(|&price| price != 0) else {
            return Some(row);
        };
        row.event.cents = cents as u32;
        row.event.category = catalog
            .products
            .get(&keys.intern(product))
            .copied()
            .unwrap_or(catalog.unknown);
    } else if kind != b"view" && kind != b"cart" {
        return Some(row);
    }
    row.user = keys.intern(user);
    row.event.ts = ts;
    Some(row)
}

pub fn person_shard(key: Key) -> usize {
    key.hash() & (PERSON_SHARDS - 1)
}

pub fn gather_shards<T>(workers: Vec<Vec<Vec<T>>>, count: usize) -> Vec<Vec<Vec<T>>> {
    let mut shards: Vec<Vec<Vec<T>>> = (0..count).map(|_| Vec::new()).collect();
    for worker in workers {
        for (shard, part) in shards.iter_mut().zip(worker) {
            if !part.is_empty() {
                shard.push(part);
            }
        }
    }
    shards
}

fn dedup_shard(parts: Vec<Vec<CopyRow>>, aliases: &FxHashMap<Key, Key>) -> Vec<Vec<PersonEvent>> {
    let count: usize = parts.iter().map(Vec::len).sum();
    let slots = (count * 8).max(4).next_power_of_two();
    let mut counts = vec![0u8; slots / 4];
    let slot = |id: Key| (id.hash() >> EVENT_SHARDS.trailing_zeros()) & (slots - 1);
    for row in parts.iter().flatten() {
        let index = slot(row.id);
        let shift = (index % 4) * 2;
        let byte = &mut counts[index / 4];
        if ((*byte >> shift) & 0b11) < 2 {
            *byte += 1 << shift;
        }
    }
    let mut output: Vec<Vec<PersonEvent>> = (0..PERSON_SHARDS).map(|_| Vec::new()).collect();
    let mut emit = |row: CopyRow| {
        if row.event.ts != NO_TIME {
            let person = aliases.get(&row.user).copied().unwrap_or(row.user);
            output[person_shard(person)].push(PersonEvent {
                person,
                event: row.event,
            });
        }
    };
    let mut collisions = FxHashMap::<Key, CopyRow>::default();
    for row in parts.into_iter().flatten() {
        let index = slot(row.id);
        if ((counts[index / 4] >> ((index % 4) * 2)) & 0b11) == 1 {
            // Equal IDs always hit the same slot. Collisions only send extra rows to the exact map.
            emit(row);
        } else {
            let first = collisions.entry(row.id).or_insert(row);
            if row.ingest < first.ingest {
                *first = row;
            }
        }
    }
    collisions.into_values().for_each(emit);
    output
}

pub fn read_copies(
    data: &Path,
    keys: &Keys,
    window: Window,
    catalog: &Catalog,
    workers: usize,
) -> io::Result<Vec<Vec<Vec<CopyRow>>>> {
    scan(
        &csv_files(&data.join("events"))?,
        workers,
        || (0..EVENT_SHARDS).map(|_| Vec::new()).collect::<Vec<_>>(),
        |shards, line| {
            if let Some(row) = parse_event(line, keys, window, catalog) {
                shards[row.id.hash() & (EVENT_SHARDS - 1)].push(row);
            }
        },
    )
}

pub fn survivors(
    copies: Vec<Vec<Vec<CopyRow>>>,
    aliases: &FxHashMap<Key, Key>,
) -> Vec<Vec<Vec<PersonEvent>>> {
    let deduped = gather_shards(copies, EVENT_SHARDS)
        .into_par_iter()
        .map(|parts| dedup_shard(parts, aliases))
        .collect();
    gather_shards(deduped, PERSON_SHARDS)
}
