use std::io;
use std::path::Path;

use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::events::{Catalog, PERSON_SHARDS, gather_shards, person_shard};
use crate::input::{csv_files, fields, scan};
use crate::model::{Key, Keys, WEEK, Window, timestamp};
use crate::sessions::{Session, Timeline};

#[derive(Clone, Copy, Default)]
struct Click {
    id: Key,
    ts: i64,
    person: u32,
    campaign: u32,
}

#[derive(Clone, Copy, Default)]
pub struct Metrics {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assists: i64,
}

pub struct Totals {
    first_day: i32,
    days: usize,
    dense: Vec<Metrics>,
    sparse: FxHashMap<(u32, i32), Metrics>,
}

impl Totals {
    fn new(window: Window, campaigns: usize) -> Self {
        let cells = window.days().saturating_mul(campaigns);
        Self {
            first_day: window.first_day(),
            days: window.days(),
            dense: if cells <= 65_536 {
                vec![Metrics::default(); cells]
            } else {
                Vec::new()
            },
            sparse: FxHashMap::default(),
        }
    }

    fn cell(&mut self, campaign: u32, day: i32) -> &mut Metrics {
        if self.dense.is_empty() {
            self.sparse.entry((campaign, day)).or_default()
        } else {
            &mut self.dense[campaign as usize * self.days + (day - self.first_day) as usize]
        }
    }

    fn merge(mut self, other: Self) -> Self {
        let add = |a: &mut Metrics, b: Metrics| {
            a.sessions += b.sessions;
            a.purchasing += b.purchasing;
            a.revenue += b.revenue;
            a.assists += b.assists;
        };
        for (a, b) in self.dense.iter_mut().zip(other.dense) {
            add(a, b);
        }
        for ((campaign, day), values) in other.sparse {
            add(self.cell(campaign, day), values);
        }
        self
    }

    pub fn rows(&self) -> impl Iterator<Item = (u32, i32, Metrics)> + '_ {
        self.dense
            .iter()
            .enumerate()
            .map(|(index, &values)| {
                let campaign = (index / self.days) as u32;
                let day = self.first_day + (index % self.days) as i32;
                (campaign, day, values)
            })
            .chain(
                self.sparse
                    .iter()
                    .map(|(&(campaign, day), &values)| (campaign, day, values)),
            )
            .filter(|(_, _, values)| values.sessions != 0 || values.assists != 0)
    }
}

fn mask_in_word(word: usize, first: usize, end: usize) -> u64 {
    let lo = first.saturating_sub(word * 64);
    let hi = (end - word * 64).min(64);
    (u64::MAX << lo) & (u64::MAX >> (64 - hi))
}

#[derive(Default)]
struct Coverage {
    campaigns: Vec<u32>,
    positions: Vec<usize>,
    masks: Vec<u64>,
    latest: Vec<Option<Click>>,
    days: Vec<(i32, usize, usize)>,
}

impl Coverage {
    fn apply(&mut self, sessions: &[Session], clicks: &[Click], keys: &Keys, totals: &mut Totals) {
        let words = sessions.len().div_ceil(64);
        self.latest.clear();
        self.latest.resize(sessions.len(), None);
        self.days.clear();
        for (i, &session) in sessions.iter().enumerate() {
            let day = session.day();
            match self.days.last_mut() {
                Some((previous, _, end)) if day == *previous => *end = i + 1,
                _ => self.days.push((day, i, i + 1)),
            }
        }
        for &click in clicks {
            let first = sessions.partition_point(|session| session.start < click.ts);
            if first == sessions.len() || sessions[first].start - click.ts > WEEK {
                continue;
            }
            let end = first
                + sessions[first..].partition_point(|session| session.start - click.ts <= WEEK);
            // Later clicks with the same first eligible session expire later, so they dominate credit.
            let recent = &mut self.latest[first];
            if recent.is_none_or(|prior| {
                click.ts > prior.ts
                    || (click.ts == prior.ts && keys.cmp(click.id, prior.id).is_lt())
            }) {
                *recent = Some(click);
            }
            let position = &mut self.positions[click.campaign as usize];
            if *position == usize::MAX {
                *position = self.masks.len();
                self.masks.resize(*position + words, 0);
                self.campaigns.push(click.campaign);
            }
            for word in first / 64..=(end - 1) / 64 {
                self.masks[*position + word] |= mask_in_word(word, first, end);
            }
        }

        let mut latest = None;
        for (&session, &candidate) in sessions.iter().zip(&self.latest) {
            if candidate.is_some() {
                latest = candidate;
            }
            let campaign = latest
                .filter(|click| session.start - click.ts <= WEEK)
                .map_or(0, |click| click.campaign);
            let metrics = totals.cell(campaign, session.day());
            metrics.sessions += 1;
            metrics.purchasing += i64::from(session.revenue > 0);
            metrics.revenue += session.revenue;
        }

        for campaign in self.campaigns.drain(..) {
            let position = &mut self.positions[campaign as usize];
            for &(day, first, end) in &self.days {
                let count: i64 = (first / 64..=(end - 1) / 64)
                    .map(|word| {
                        i64::from(
                            (self.masks[*position + word] & mask_in_word(word, first, end))
                                .count_ones(),
                        )
                    })
                    .sum();
                if count != 0 {
                    totals.cell(campaign, day).assists += count;
                }
            }
            *position = usize::MAX;
        }
        self.masks.clear();
    }
}

fn summarize(
    timeline: &Timeline,
    parts: Vec<Vec<Click>>,
    keys: &Keys,
    totals: &mut Totals,
    coverage: &mut Coverage,
) {
    let mut counts = vec![0usize; timeline.index.len()];
    for click in parts.iter().flatten() {
        counts[click.person as usize] += 1;
    }
    let mut starts = Vec::with_capacity(counts.len() + 1);
    starts.push(0);
    for count in counts {
        starts.push(starts.last().unwrap() + count);
    }
    let mut next = starts.clone();
    let mut grouped = vec![Click::default(); *starts.last().unwrap()];
    for click in parts.into_iter().flatten() {
        let position = &mut next[click.person as usize];
        grouped[*position] = click;
        *position += 1;
    }
    for (sessions, clicks) in timeline.offsets.windows(2).zip(starts.windows(2)) {
        let sessions = &timeline.sessions[sessions[0]..sessions[1]];
        let clicks = &grouped[clicks[0]..clicks[1]];
        coverage.apply(sessions, clicks, keys, totals);
    }
}

pub fn aggregate(
    data: &Path,
    keys: &Keys,
    window: Window,
    catalog: &Catalog,
    aliases: &FxHashMap<Key, Key>,
    timelines: &[Timeline],
    workers: usize,
) -> io::Result<Totals> {
    let workers = scan(
        &csv_files(&data.join("clicks"))?,
        workers,
        || (0..PERSON_SHARDS).map(|_| Vec::new()).collect::<Vec<_>>(),
        |parts, line| {
            let Some([id, user, campaign, ts]) = fields(line) else {
                return;
            };
            if id.is_empty() || user.is_empty() {
                return;
            }
            let user = keys.intern(user);
            let user = aliases.get(&user).copied().unwrap_or(user);
            let shard = person_shard(user);
            let Some(&person) = timelines[shard].index.get(&user) else {
                return;
            };
            let Some(&campaign) = catalog.campaigns.get(&keys.intern(campaign)) else {
                return;
            };
            let Some(ts) = timestamp(ts).filter(|&ts| ts >= window.start - WEEK && ts < window.end)
            else {
                return;
            };
            parts[shard].push(Click {
                id: keys.intern(id),
                ts,
                person: person as u32,
                campaign,
            });
        },
    )?;
    let campaign_count = catalog.campaign_names.len();
    let totals = timelines
        .par_iter()
        .zip(gather_shards(workers, PERSON_SHARDS))
        .fold(
            || {
                (
                    Totals::new(window, campaign_count),
                    Coverage {
                        positions: vec![usize::MAX; campaign_count],
                        ..Coverage::default()
                    },
                )
            },
            |(mut totals, mut coverage), (timeline, parts)| {
                summarize(timeline, parts, keys, &mut totals, &mut coverage);
                (totals, coverage)
            },
        )
        .map(|(totals, _)| totals)
        .reduce(|| Totals::new(window, campaign_count), Totals::merge);
    Ok(totals)
}
