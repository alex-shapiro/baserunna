//! Query values and aggregate states. Every aggregate is a state with
//! `add`, `merge` and `finalize` (ClickHouse's `-State` and `-Merge`), so
//! partial states computed per part or per unsealed batch combine into the
//! same answer however the rows were split.

use super::query::{AggFn, FunnelMode};
use super::sketch::{DdSketch, ExactSum, Hll, hash64};
use crate::engine::events::Pos;
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use serde_json::{Value as Json, json};
use std::cmp::Ordering;
use std::collections::BTreeSet;

/// A value a query evaluates or returns.
#[derive(Clone, Debug)]
pub enum Val {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// An array or object from a JSON column.
    Json(Json),
    /// An aggregate's array result.
    List(Vec<Val>),
}

impl PartialEq for Val {
    fn eq(&self, o: &Val) -> bool {
        self.canonical_bytes() == o.canonical_bytes()
    }
}

impl Val {
    pub fn from_json(j: &Json) -> Val {
        match j {
            Json::Null => Val::Null,
            Json::Bool(b) => Val::Bool(*b),
            Json::Number(n) => match n.as_i64() {
                Some(i) => Val::Int(i),
                None => n.as_f64().map_or(Val::Null, Val::Float),
            },
            Json::String(s) => Val::Str(s.clone()),
            other => Val::Json(other.clone()),
        }
    }

    pub fn to_json(&self) -> Json {
        match self {
            Val::Null => Json::Null,
            Val::Bool(b) => json!(b),
            Val::Int(i) => json!(i),
            Val::Float(f) if f.is_finite() => json!(f),
            Val::Float(_) => Json::Null,
            Val::Str(s) => json!(s),
            Val::Json(j) => j.clone(),
            Val::List(v) => Json::Array(v.iter().map(Val::to_json).collect()),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Val::Null)
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Val::Int(i) => Some(*i as f64),
            Val::Float(f) => Some(*f),
            Val::Json(j) => j.as_f64(),
            _ => None,
        }
    }

    /// The value with integral floats as ints, so that equal numbers group
    /// and print alike however they were stored.
    pub fn canonical(self) -> Val {
        match self {
            Val::Float(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => Val::Int(f as i64),
            Val::List(v) => Val::List(v.into_iter().map(Val::canonical).collect()),
            v => v,
        }
    }

    /// Bytes equal exactly for values that compare equal.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        self.put_key(&mut out);
        out
    }

    fn put_key(&self, out: &mut Vec<u8>) {
        match self {
            Val::Null => out.push(0),
            Val::Bool(b) => out.extend([1, *b as u8]),
            Val::Int(i) => {
                out.push(2);
                out.extend(i.to_be_bytes());
            }
            Val::Float(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => {
                out.push(2);
                out.extend((*f as i64).to_be_bytes());
            }
            Val::Float(f) => {
                out.push(3);
                out.extend((if *f == 0.0 { 0.0 } else { *f }).to_bits().to_be_bytes());
            }
            Val::Str(s) => {
                out.push(4);
                out.extend(s.as_bytes());
                out.push(0);
            }
            Val::Json(j) => {
                out.push(5);
                out.extend(j.to_string().as_bytes());
                out.push(0);
            }
            Val::List(v) => {
                out.push(6);
                out.extend((v.len() as u32).to_be_bytes());
                v.iter().for_each(|x| x.put_key(out));
            }
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Val::Null => 0,
            Val::Bool(_) => 1,
            Val::Int(_) | Val::Float(_) => 2,
            Val::Str(_) => 3,
            Val::Json(_) => 4,
            Val::List(_) => 5,
        }
    }

    /// A total order: null, booleans, numbers by value, strings by bytes,
    /// then JSON and lists by their canonical bytes.
    pub fn total_cmp(&self, o: &Val) -> Ordering {
        match (self, o) {
            (Val::Bool(a), Val::Bool(b)) => a.cmp(b),
            (Val::Int(a), Val::Int(b)) => a.cmp(b),
            (Val::Int(_) | Val::Float(_), Val::Int(_) | Val::Float(_)) => {
                let (a, b) = (self.as_f64().unwrap_or(0.0), o.as_f64().unwrap_or(0.0));
                a.total_cmp(&b)
            }
            (Val::Str(a), Val::Str(b)) => a.as_bytes().cmp(b.as_bytes()),
            _ if self.rank() == o.rank() => self.canonical_bytes().cmp(&o.canonical_bytes()),
            _ => self.rank().cmp(&o.rank()),
        }
    }

    fn encode(&self, w: &mut Writer) {
        match self {
            Val::Null => w.u8(0),
            Val::Bool(b) => {
                w.u8(1);
                w.bool(*b);
            }
            Val::Int(i) => {
                w.u8(2);
                w.u64(*i as u64);
            }
            Val::Float(f) => {
                w.u8(3);
                w.u64(f.to_bits());
            }
            Val::Str(s) => {
                w.u8(4);
                w.str(s);
            }
            Val::Json(j) => {
                w.u8(5);
                w.str(&j.to_string());
            }
            Val::List(v) => {
                w.u8(6);
                w.varint(v.len() as u64);
                v.iter().for_each(|x| x.encode(w));
            }
        }
    }

    fn decode(r: &mut Reader<'_>, depth: u32) -> DResult<Val> {
        if depth > 4 {
            return Err(DecodeError("value nests too deep"));
        }
        Ok(match r.u8()? {
            0 => Val::Null,
            1 => Val::Bool(r.bool()?),
            2 => Val::Int(r.u64()? as i64),
            3 => Val::Float(f64::from_bits(r.u64()?)),
            4 => Val::Str(r.string()?),
            5 => Val::Json(serde_json::from_str(r.str()?).map_err(|_| DecodeError("bad json value"))?),
            6 => {
                let n = r.len(r.remaining())?;
                (0..n).map(|_| Val::decode(r, depth + 1)).collect::<DResult<Vec<_>>>().map(Val::List)?
            }
            _ => return Err(DecodeError("bad value tag")),
        })
    }
}

/// An exact sum of integers and floats.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SumState {
    ints: i128,
    floats: ExactSum,
    float: bool,
}

impl SumState {
    fn add(&mut self, v: &Val) -> bool {
        match v {
            Val::Int(i) => self.ints += *i as i128,
            Val::Bool(b) => self.ints += *b as i128,
            Val::Float(f) => {
                self.floats.add(*f);
                self.float = true;
            }
            Val::Json(j) => match Val::from_json(j) {
                Val::Json(_) => return false,
                x => return self.add(&x),
            },
            _ => return false,
        }
        true
    }

    fn merge(&mut self, o: &SumState) {
        self.ints += o.ints;
        self.floats.merge(&o.floats);
        self.float |= o.float;
    }

    fn exact(&self) -> ExactSum {
        let mut s = self.floats.clone();
        let mut rest = self.ints;
        for shift in [80u32, 40, 0] {
            let part = rest >> shift;
            rest -= part << shift;
            s.add(part as f64 * 2f64.powi(shift as i32));
        }
        s
    }

    fn value(&self) -> Val {
        if !self.float {
            if let Ok(i) = i64::try_from(self.ints) {
                return Val::Int(i);
            }
        }
        Val::Float(self.exact().value())
    }
}

/// An aggregate's partial state.
#[derive(Clone, Debug, PartialEq)]
pub enum State {
    Count(u64),
    Sum(SumState),
    /// Element-wise sums of arrays, as of retention results.
    SumList(Vec<SumState>),
    Min(Option<Val>),
    Max(Option<Val>),
    Avg(SumState, u64),
    Uniq(Hll),
    UniqExact(BTreeSet<Vec<u8>>),
    Quantiles(DdSketch),
    /// `(time, position, steps matched)` of each event of the group.
    Funnel(Vec<(i64, Pos, u32)>),
    Retention(u32),
}

impl State {
    pub fn new(f: &AggFn) -> State {
        match f {
            AggFn::Count => State::Count(0),
            AggFn::Sum => State::Sum(SumState::default()),
            AggFn::Min => State::Min(None),
            AggFn::Max => State::Max(None),
            AggFn::Avg => State::Avg(SumState::default(), 0),
            AggFn::Uniq => State::Uniq(Hll::default()),
            AggFn::UniqExact => State::UniqExact(BTreeSet::new()),
            AggFn::Quantiles(_) => State::Quantiles(DdSketch::default()),
            AggFn::WindowFunnel { .. } => State::Funnel(Vec::new()),
            AggFn::Retention(_) => State::Retention(0),
        }
    }

    /// Adds one row's value; funnels and retention take their events and
    /// condition masks through `event` and `mask` instead.
    pub fn add(&mut self, v: &Val, event: Option<(i64, Pos, u32)>) {
        match self {
            State::Count(n) => *n += 1,
            State::Sum(s) => {
                if let Val::List(items) = v {
                    let mut list = vec![SumState::default(); items.len()];
                    for (st, x) in list.iter_mut().zip(items) {
                        st.add(x);
                    }
                    *self = State::SumList(list);
                } else {
                    s.add(v);
                }
            }
            State::SumList(list) => {
                if let Val::List(items) = v {
                    if list.len() < items.len() {
                        list.resize(items.len(), SumState::default());
                    }
                    for (st, x) in list.iter_mut().zip(items) {
                        st.add(x);
                    }
                }
            }
            State::Min(m) => {
                if !v.is_null() && m.as_ref().is_none_or(|x| v.total_cmp(x) == Ordering::Less) {
                    *m = Some(v.clone().canonical());
                }
            }
            State::Max(m) => {
                if !v.is_null() && m.as_ref().is_none_or(|x| v.total_cmp(x) == Ordering::Greater) {
                    *m = Some(v.clone().canonical());
                }
            }
            State::Avg(s, n) => {
                if s.add(v) {
                    *n += 1;
                }
            }
            State::Uniq(h) => {
                if !v.is_null() {
                    h.add_hash(hash64(&v.canonical_bytes()));
                }
            }
            State::UniqExact(set) => {
                if !v.is_null() {
                    set.insert(v.canonical_bytes());
                }
            }
            State::Quantiles(d) => {
                if let Some(f) = v.as_f64() {
                    d.add(f);
                }
            }
            State::Funnel(evs) => evs.extend(event),
            State::Retention(m) => *m |= event.map_or(0, |e| e.2),
        }
    }

    pub fn merge(&mut self, o: &State) {
        match (&mut *self, o) {
            (State::Count(a), State::Count(b)) => *a += b,
            (State::Sum(a), State::Sum(b)) => a.merge(b),
            (State::SumList(a), State::SumList(b)) => {
                if a.len() < b.len() {
                    a.resize(b.len(), SumState::default());
                }
                a.iter_mut().zip(b).for_each(|(x, y)| x.merge(y));
            }
            (State::Sum(a), State::SumList(b)) if *a == SumState::default() => *self = State::SumList(b.clone()),
            (State::SumList(_), State::Sum(b)) if *b == SumState::default() => {}
            (State::Min(a), State::Min(Some(b))) => {
                if a.as_ref().is_none_or(|x| b.total_cmp(x) == Ordering::Less) {
                    *a = Some(b.clone());
                }
            }
            (State::Max(a), State::Max(Some(b))) => {
                if a.as_ref().is_none_or(|x| b.total_cmp(x) == Ordering::Greater) {
                    *a = Some(b.clone());
                }
            }
            (State::Min(_) | State::Max(_), _) => {}
            (State::Avg(a, n), State::Avg(b, m)) => {
                a.merge(b);
                *n += m;
            }
            (State::Uniq(a), State::Uniq(b)) => a.merge(b),
            (State::UniqExact(a), State::UniqExact(b)) => a.extend(b.iter().cloned()),
            (State::Quantiles(a), State::Quantiles(b)) => a.merge(b),
            (State::Funnel(a), State::Funnel(b)) => a.extend_from_slice(b),
            (State::Retention(a), State::Retention(b)) => *a |= b,
            _ => {}
        }
    }

    pub fn finalize(&self, f: &AggFn) -> Val {
        match (self, f) {
            (State::Count(n), _) => Val::Int(*n as i64),
            (State::Sum(s), _) => s.value(),
            (State::SumList(l), _) => Val::List(l.iter().map(SumState::value).collect()),
            (State::Min(m) | State::Max(m), _) => m.clone().unwrap_or(Val::Null),
            (State::Avg(_, 0), _) => Val::Null,
            (State::Avg(s, n), _) => Val::Float(s.exact().value() / *n as f64),
            (State::Uniq(h), _) => Val::Int(h.estimate() as i64),
            (State::UniqExact(s), _) => Val::Int(s.len() as i64),
            (State::Quantiles(d), AggFn::Quantiles(qs)) => {
                Val::List(qs.iter().map(|q| d.quantile(*q).map_or(Val::Null, Val::Float)).collect())
            }
            (State::Funnel(evs), AggFn::WindowFunnel { window_ms, steps, mode }) => {
                Val::Int(funnel(evs, steps.len(), *window_ms, *mode) as i64)
            }
            (State::Retention(m), AggFn::Retention(c)) => {
                let first = m & 1 != 0;
                Val::List((0..c.len()).map(|i| Val::Bool(first && m & (1 << i) != 0)).collect())
            }
            _ => Val::Null,
        }
    }

    /// Approximate bytes of memory the state holds.
    pub fn mem_size(&self) -> usize {
        32 + match self {
            State::SumList(l) => l.len() * 64,
            State::Min(Some(v)) | State::Max(Some(v)) => v.canonical_bytes().len(),
            State::Uniq(h) => h.mem_size(),
            State::UniqExact(s) => s.iter().map(|b| b.len() + 32).sum(),
            State::Quantiles(d) => d.mem_size(),
            State::Funnel(e) => e.len() * 32,
            _ => 32,
        }
    }

    pub fn encode(&self, w: &mut Writer) {
        let sum = |w: &mut Writer, s: &SumState| {
            w.u64(s.ints as u64);
            w.u64((s.ints >> 64) as u64);
            s.floats.encode(w);
            w.bool(s.float);
        };
        match self {
            State::Count(n) => {
                w.u8(0);
                w.varint(*n);
            }
            State::Sum(s) => {
                w.u8(1);
                sum(w, s);
            }
            State::SumList(l) => {
                w.u8(2);
                w.varint(l.len() as u64);
                l.iter().for_each(|s| sum(w, s));
            }
            State::Min(m) | State::Max(m) => {
                w.u8(if matches!(self, State::Min(_)) { 3 } else { 4 });
                m.clone().unwrap_or(Val::Null).encode(w);
            }
            State::Avg(s, n) => {
                w.u8(5);
                sum(w, s);
                w.varint(*n);
            }
            State::Uniq(h) => {
                w.u8(6);
                h.encode(w);
            }
            State::UniqExact(set) => {
                w.u8(7);
                w.varint(set.len() as u64);
                set.iter().for_each(|b| w.bytes(b));
            }
            State::Quantiles(d) => {
                w.u8(8);
                d.encode(w);
            }
            State::Funnel(evs) => {
                w.u8(9);
                w.varint(evs.len() as u64);
                for (t, p, m) in evs {
                    w.u64(*t as u64);
                    w.varint(p.version);
                    w.varint(p.ordinal as u64);
                    w.u32(*m);
                }
            }
            State::Retention(m) => {
                w.u8(10);
                w.u32(*m);
            }
        }
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<State> {
        let sum = |r: &mut Reader<'_>| -> DResult<SumState> {
            let lo = r.u64()? as u128;
            let hi = r.u64()? as u128;
            Ok(SumState { ints: (lo | (hi << 64)) as i128, floats: ExactSum::decode(r)?, float: r.bool()? })
        };
        let opt = |v: Val| (!v.is_null()).then_some(v);
        Ok(match r.u8()? {
            0 => State::Count(r.varint()?),
            1 => State::Sum(sum(r)?),
            2 => {
                let n = r.len(r.remaining())?;
                State::SumList((0..n).map(|_| sum(r)).collect::<DResult<_>>()?)
            }
            3 => State::Min(opt(Val::decode(r, 0)?)),
            4 => State::Max(opt(Val::decode(r, 0)?)),
            5 => State::Avg(sum(r)?, r.varint()?),
            6 => State::Uniq(Hll::decode(r)?),
            7 => {
                let n = r.len(r.remaining())?;
                State::UniqExact((0..n).map(|_| r.bytes().map(<[u8]>::to_vec)).collect::<DResult<_>>()?)
            }
            8 => State::Quantiles(DdSketch::decode(r)?),
            9 => {
                let n = r.len(r.remaining() / 6)?;
                let mut evs = Vec::with_capacity(n);
                for _ in 0..n {
                    let t = r.u64()? as i64;
                    let version = r.varint()?;
                    let ordinal = u32::try_from(r.varint()?).map_err(|_| DecodeError("bad ordinal"))?;
                    evs.push((t, Pos { version, ordinal }, r.u32()?));
                }
                State::Funnel(evs)
            }
            10 => State::Retention(r.u32()?),
            _ => return Err(DecodeError("bad aggregate state")),
        })
    }
}

/// The number of funnel steps reached (ClickHouse's `windowFunnel`).
///
/// Events are taken in time order, then insertion order; within one event,
/// later steps are tried first so that one event never fulfills two
/// consecutive steps. A chain starts at each event matching the first step
/// and reaches step `l` from a chain at step `l - 1` whose first event lies
/// within `window_ms`.
pub fn funnel(evs: &[(i64, Pos, u32)], steps: usize, window_ms: i64, mode: FunnelMode) -> usize {
    let mut evs = evs.to_vec();
    evs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    // chains[l] = (first time, last time) of the latest chain that reached step l + 1.
    let mut chains: Vec<Option<(i64, i64)>> = vec![None; steps];
    let reached = |c: &[Option<(i64, i64)>]| c.iter().rposition(Option::is_some).map_or(0, |l| l + 1);
    for (t, _, mask) in evs {
        let deepest = reached(&chains);
        if mode.strict_order && deepest > 0 && deepest < steps && mask & (1 << deepest) == 0 && mask & 1 == 0 {
            return deepest;
        }
        if mode.strict_deduplication && deepest > 0 && mask & (1 << (deepest - 1)) != 0 && mask & (1 << deepest.min(steps - 1)) == 0
        {
            return deepest;
        }
        for l in (0..steps).rev() {
            if mask & (1 << l) == 0 {
                continue;
            }
            if l == 0 {
                chains[0] = Some((t, t));
                continue;
            }
            if let Some((first, last)) = chains[l - 1]
                && t <= first.saturating_add(window_ms)
                && (!mode.strict_increase || last < t)
            {
                chains[l] = Some((first, t));
                if l + 1 == steps {
                    return steps;
                }
            }
        }
    }
    reached(&chains)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::rng::Rng;

    fn ev(t: i64, ordinal: u32, mask: u32) -> (i64, Pos, u32) {
        (t, Pos { version: 1, ordinal }, mask)
    }

    #[test]
    fn funnels_follow_windows_and_modes() {
        let m = FunnelMode::default();
        assert_eq!(funnel(&[], 3, 10, m), 0);
        assert_eq!(funnel(&[ev(0, 0, 1), ev(5, 1, 2), ev(9, 2, 4)], 3, 10, m), 3);
        assert_eq!(funnel(&[ev(0, 0, 1), ev(5, 1, 2), ev(11, 2, 4)], 3, 10, m), 2, "outside the window");
        assert_eq!(funnel(&[ev(0, 0, 1), ev(8, 1, 1), ev(12, 2, 2), ev(17, 3, 4)], 3, 10, m), 3, "a later start fits");
        assert_eq!(funnel(&[ev(5, 1, 2), ev(9, 2, 4), ev(0, 0, 1)], 3, 10, m), 3, "events are sorted by time");
        assert_eq!(funnel(&[ev(0, 0, 3)], 2, 10, m), 1, "one event fulfills one step");
        assert_eq!(funnel(&[ev(0, 0, 1), ev(0, 1, 2)], 2, 10, m), 2, "ties keep insertion order");
        let inc = FunnelMode { strict_increase: true, ..m };
        assert_eq!(funnel(&[ev(0, 0, 1), ev(0, 1, 2)], 2, 10, inc), 1);
        let order = FunnelMode { strict_order: true, ..m };
        assert_eq!(funnel(&[ev(0, 0, 1), ev(1, 1, 2), ev(2, 2, 0), ev(3, 3, 4)], 3, 10, order), 2);
        assert_eq!(funnel(&[ev(0, 0, 1), ev(1, 1, 2), ev(2, 2, 0), ev(3, 3, 4)], 3, 10, m), 3);
        let dedup = FunnelMode { strict_deduplication: true, ..m };
        assert_eq!(funnel(&[ev(0, 0, 1), ev(1, 1, 1), ev(2, 2, 2)], 2, 10, dedup), 1);
    }

    fn sample_states() -> Vec<(AggFn, Vec<(Val, Option<(i64, Pos, u32)>)>)> {
        let mut r = Rng::new(7);
        let vals = |r: &mut Rng| -> Vec<(Val, Option<(i64, Pos, u32)>)> {
            (0..r.usize(50))
                .map(|i| {
                    let v = match r.usize(5) {
                        0 => Val::Int(r.below(100) as i64 - 50),
                        1 => Val::Float((r.unit() - 0.5) * 1e3),
                        2 => Val::Str(format!("s{}", r.below(10))),
                        3 => Val::Null,
                        _ => Val::Bool(r.chance(0.5)),
                    };
                    (v, Some((r.below(100) as i64, Pos { version: 1, ordinal: i as u32 }, r.below(8) as u32)))
                })
                .collect()
        };
        let funnel_fn = AggFn::WindowFunnel { window_ms: 30, steps: vec![], mode: FunnelMode::default() };
        [AggFn::Count, AggFn::Sum, AggFn::Min, AggFn::Max, AggFn::Avg, AggFn::Uniq, AggFn::UniqExact, AggFn::Quantiles(vec![0.5, 0.9]), funnel_fn, AggFn::Retention(vec![])]
            .into_iter()
            .map(|f| (f, vals(&mut r)))
            .collect()
    }

    #[test]
    fn states_merge_in_any_grouping_and_order() {
        let mut r = Rng::new(11);
        for _ in 0..50 {
            for (f, rows) in sample_states() {
                let f = match f {
                    AggFn::WindowFunnel { .. } => AggFn::WindowFunnel {
                        window_ms: 30,
                        steps: vec![crate::filter::Filter::And(vec![]); 3],
                        mode: FunnelMode::default(),
                    },
                    AggFn::Retention(_) => AggFn::Retention(vec![crate::filter::Filter::And(vec![]); 3]),
                    f => f,
                };
                let mut whole = State::new(&f);
                rows.iter().for_each(|(v, e)| whole.add(v, *e));
                let mut parts: Vec<State> = (0..1 + r.usize(4)).map(|_| State::new(&f)).collect();
                for (v, e) in &rows {
                    let i = r.usize(parts.len());
                    parts[i].add(v, *e);
                }
                r.shuffle(&mut parts);
                let mut acc = State::new(&f);
                for p in &parts {
                    let mut w = Writer::new();
                    p.encode(&mut w);
                    let back = State::decode(&mut Reader::new(&w.buf)).unwrap();
                    acc.merge(&back);
                }
                assert_eq!(acc.finalize(&f), whole.finalize(&f), "{f:?}");
            }
        }
    }

    #[test]
    fn values_order_and_canonicalize() {
        assert_eq!(Val::Float(2.0), Val::Int(2));
        assert_ne!(Val::Float(2.5), Val::Int(2));
        assert_eq!(Val::Int(1).total_cmp(&Val::Float(1.5)), Ordering::Less);
        assert_eq!(Val::Null.total_cmp(&Val::Bool(false)), Ordering::Less);
        assert_eq!(Val::Float(3.0).canonical().to_json(), json!(3));
        let mut s = State::new(&AggFn::Sum);
        s.add(&Val::List(vec![Val::Bool(true), Val::Bool(false)]), None);
        s.add(&Val::List(vec![Val::Bool(true), Val::Bool(true)]), None);
        assert_eq!(s.finalize(&AggFn::Sum).to_json(), json!([2, 1]));
    }

    #[test]
    fn fuzz_state_decoder_never_panics() {
        let mut r = Rng::new(5);
        for _ in 0..20_000 {
            let n = r.usize(64);
            let b = r.bytes(n);
            let _ = State::decode(&mut Reader::new(&b));
        }
    }
}
