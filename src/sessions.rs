use rustc_hash::FxHashMap;

use crate::events::Event;
use crate::events::PersonEvent;
use crate::model::{DAY, GAP, Key, NO_CATEGORY, Window};

// Tiny event lists are cheap to sort. Cap dense buckets for unusually long windows.
const MIN_BUCKET_EVENTS: usize = 64;
const MAX_DENSE_BUCKETS: usize = 8192;

#[derive(Clone, Copy, Default)]
pub struct Session {
    pub start: i64,
    pub revenue: i64,
    events: i64,
}

impl Session {
    pub fn day(self) -> i32 {
        self.start.div_euclid(DAY) as i32
    }
}

pub struct UserDay {
    pub person: Key,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub category: u32,
}

pub struct Timeline {
    pub index: FxHashMap<Key, usize>,
    pub offsets: Vec<usize>,
    pub sessions: Vec<Session>,
    pub user_days: Vec<UserDay>,
}

#[derive(Clone, Copy, Default)]
struct Bucket {
    first: i64,
    last: i64,
    count: i64,
    revenue: i64,
    start_day: i32,
}

#[derive(Default)]
struct Scratch {
    buckets: Vec<Bucket>,
    occupied: Vec<usize>,
    purchases: Vec<(i32, u32, u32)>,
}

impl Scratch {
    fn sorted_sessions(&mut self, events: &mut [Event], sessions: &mut Vec<Session>) {
        events.sort_unstable_by_key(|event| event.ts);
        let mut session = Session {
            start: events[0].ts,
            ..Session::default()
        };
        let mut previous = session.start;
        for &Event {
            ts,
            cents,
            category,
        } in events.iter()
        {
            if ts - previous > GAP {
                sessions.push(session);
                session = Session {
                    start: ts,
                    ..Session::default()
                };
            }
            session.events += 1;
            session.revenue += i64::from(cents);
            if cents != 0 {
                self.purchases.push((session.day(), category, cents));
            }
            previous = ts;
        }
        sessions.push(session);
    }

    fn bucket_sessions(
        &mut self,
        events: &[Event],
        sessions: &mut Vec<Session>,
        window: Window,
        count: usize,
    ) {
        self.buckets.resize(count, Bucket::default());
        let bucket_index = |ts| ((ts - window.start) / GAP) as usize;
        for &Event { ts, cents, .. } in events {
            let index = bucket_index(ts);
            let bucket = &mut self.buckets[index];
            if bucket.count == 0 {
                self.occupied.push(index);
                bucket.first = ts;
                bucket.last = ts;
            } else {
                bucket.first = bucket.first.min(ts);
                bucket.last = bucket.last.max(ts);
            }
            bucket.count += 1;
            bucket.revenue += i64::from(cents);
        }
        self.occupied.sort_unstable();
        let first = self.buckets[self.occupied[0]].first;
        let mut session = Session {
            start: first,
            ..Session::default()
        };
        let mut previous = first;
        for &index in &self.occupied {
            let bucket = &mut self.buckets[index];
            // No gap within a half-open GAP-wide bucket can split a session.
            if bucket.first - previous > GAP {
                sessions.push(session);
                session = Session {
                    start: bucket.first,
                    ..Session::default()
                };
            }
            session.events += bucket.count;
            session.revenue += bucket.revenue;
            bucket.start_day = session.day();
            previous = bucket.last;
        }
        sessions.push(session);
        for &Event {
            ts,
            cents,
            category,
        } in events
        {
            if cents != 0 {
                self.purchases
                    .push((self.buckets[bucket_index(ts)].start_day, category, cents));
            }
        }
        for index in self.occupied.drain(..) {
            self.buckets[index] = Bucket::default();
        }
    }

    fn daily(&mut self, person: Key, mut sessions: &[Session], rows: &mut Vec<UserDay>) {
        self.purchases
            .sort_unstable_by_key(|&(day, category, _)| (day, category));
        let mut purchases = self.purchases.as_slice();
        while let Some(first) = sessions.first() {
            let day = first.day();
            let mut row = UserDay {
                person,
                day,
                sessions: 0,
                events: 0,
                revenue: 0,
                category: NO_CATEGORY,
            };
            while let Some((session, rest)) = sessions.split_first().filter(|(s, _)| s.day() == day)
            {
                row.sessions += 1;
                row.events += session.events;
                row.revenue += session.revenue;
                sessions = rest;
            }
            let mut best = 0;
            while let Some(&(_, category, _)) =
                purchases.first().filter(|purchase| purchase.0 == day)
            {
                let mut total = 0;
                while let Some((&(_, _, cents), rest)) = purchases
                    .split_first()
                    .filter(|(purchase, _)| purchase.0 == day && purchase.1 == category)
                {
                    total += i64::from(cents);
                    purchases = rest;
                }
                if total > best {
                    best = total;
                    row.category = category;
                }
            }
            rows.push(row);
        }
        self.purchases.clear();
    }
}

pub fn build(parts: Vec<Vec<PersonEvent>>, window: Window) -> Timeline {
    let mut people = Vec::new();
    let mut index = FxHashMap::default();
    let mut counts = Vec::new();
    for row in parts.iter().flatten() {
        let id = *index.entry(row.person).or_insert_with(|| {
            let id = people.len();
            people.push(row.person);
            counts.push(0usize);
            id
        });
        counts[id] += 1;
    }
    let mut starts = Vec::with_capacity(counts.len() + 1);
    starts.push(0);
    for count in counts {
        starts.push(starts.last().unwrap() + count);
    }
    let mut next = starts.clone();
    let mut grouped = vec![Event::default(); *starts.last().unwrap()];
    for row in parts.into_iter().flatten() {
        let target = &mut next[index[&row.person]];
        grouped[*target] = row.event;
        *target += 1;
    }

    let mut sessions = Vec::new();
    let mut offsets = Vec::with_capacity(people.len() + 1);
    let mut user_days = Vec::new();
    let mut scratch = Scratch::default();
    let bucket_count = (1 + (window.end - window.start - 1) / GAP) as usize;
    for (person, bounds) in people.into_iter().zip(starts.windows(2)) {
        let begin = sessions.len();
        offsets.push(begin);
        let events = &mut grouped[bounds[0]..bounds[1]];
        if events.len() >= MIN_BUCKET_EVENTS && bucket_count <= MAX_DENSE_BUCKETS {
            scratch.bucket_sessions(events, &mut sessions, window, bucket_count);
        } else {
            scratch.sorted_sessions(events, &mut sessions);
        }
        scratch.daily(person, &sessions[begin..], &mut user_days);
    }
    offsets.push(sessions.len());
    Timeline {
        index,
        offsets,
        sessions,
        user_days,
    }
}
