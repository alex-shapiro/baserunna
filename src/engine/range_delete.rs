//! Range tombstones: compact representations that delete all older entries in a key range (SPEC §5.5).

use super::manifest::SegmentMeta;
use super::types::{MAX_KEY_LEN, Seq};
use super::version::KeyRange;
use crate::util::codec::{DResult, DecodeError, Reader, Writer};
use bytes::Bytes;
use std::collections::BTreeSet;
use std::ops::Bound;

/// A range deletion for keys in `[start, end)`, or `[start, ∞)` when `end` is `None`, at sequence `seq`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Bytes,
    pub end: Option<Bytes>,
    pub seq: Seq,
}

/// Returns the lexicographically smallest key strictly greater than `k`.
fn successor(k: &[u8]) -> Bytes {
    let mut v = Vec::with_capacity(k.len() + 1);
    v.extend_from_slice(k);
    v.push(0);
    Bytes::from(v)
}

impl RangeTombstone {
    /// Creates a tombstone covering `r` at `seq`, or returns `None` if the range is empty.
    pub fn new(r: &KeyRange, seq: Seq) -> Option<RangeTombstone> {
        let start = match &r.start {
            Bound::Included(s) => s.clone(),
            Bound::Excluded(s) => successor(s),
            Bound::Unbounded => Bytes::new(),
        };
        let end = match &r.end {
            Bound::Included(e) => Some(successor(e)),
            Bound::Excluded(e) => Some(e.clone()),
            Bound::Unbounded => None,
        };
        let rt = RangeTombstone { start, end, seq };
        (!rt.is_empty()).then_some(rt)
    }

    fn is_empty(&self) -> bool {
        self.end.as_ref().is_some_and(|e| *e <= self.start)
    }

    pub fn range(&self) -> KeyRange {
        KeyRange::half_open(self.start.clone(), self.end.clone())
    }

    pub fn contains_key(&self, k: &[u8]) -> bool {
        k >= self.start.as_ref() && self.end.as_ref().is_none_or(|e| k < e.as_ref())
    }

    /// Returns true if this tombstone completely covers `other`.
    pub fn contains_range(&self, other: &RangeTombstone) -> bool {
        other.start >= self.start
            && match (&self.end, &other.end) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(a), Some(b)) => b <= a,
            }
    }

    /// Returns true if this tombstone overlaps the segment's key range.
    pub fn overlaps(&self, s: &SegmentMeta) -> bool {
        s.max_key.as_ref() >= self.start.as_ref() && self.end.as_ref().is_none_or(|e| s.min_key.as_ref() < e.as_ref())
    }

    /// Returns true if segment `s` may contain entries affected by this tombstone.
    pub fn dirties(&self, s: &SegmentMeta) -> bool {
        s.deletes_applied < self.seq && s.min_seq < self.seq && self.overlaps(s)
    }

    /// Returns true if this tombstone covers all entries in segment `s`.
    pub fn covers(&self, s: &SegmentMeta) -> bool {
        s.max_seq < self.seq && self.contains_key(&s.min_key) && self.contains_key(&s.max_key)
    }

    pub fn encode(&self, w: &mut Writer) {
        w.bytes(&self.start);
        match &self.end {
            None => w.u8(0),
            Some(e) => {
                w.u8(1);
                w.bytes(e);
            }
        }
        w.varint(self.seq);
    }

    pub fn decode(r: &mut Reader<'_>) -> DResult<RangeTombstone> {
        let bound = |b: &[u8]| {
            if b.len() > MAX_KEY_LEN + 1 {
                Err(DecodeError("range bound too long"))
            } else {
                Ok(Bytes::copy_from_slice(b))
            }
        };
        let start = bound(r.bytes()?)?;
        let end = match r.u8()? {
            0 => None,
            1 => Some(bound(r.bytes()?)?),
            _ => return Err(DecodeError("bad range end tag")),
        };
        let rt = RangeTombstone { start, end, seq: r.varint()? };
        if rt.is_empty() {
            return Err(DecodeError("empty range tombstone"));
        }
        Ok(rt)
    }
}

/// Range tombstones partitioned into disjoint fragments with the newest
/// covering sequence number.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RangeDeletes {
    /// Sorted, disjoint `(start, end, seq)`; at most the last is unbounded.
    frags: Vec<(Bytes, Option<Bytes>, Seq)>,
}

impl RangeDeletes {
    pub fn new(rts: &[RangeTombstone]) -> RangeDeletes {
        let mut starts: Vec<(&Bytes, usize)> = rts.iter().enumerate().map(|(i, t)| (&t.start, i)).collect();
        let mut ends: Vec<(&Bytes, usize)> =
            rts.iter().enumerate().filter_map(|(i, t)| t.end.as_ref().map(|e| (e, i))).collect();
        starts.sort();
        ends.sort();
        let mut points: Vec<&Bytes> = starts.iter().chain(&ends).map(|(p, _)| *p).collect();
        points.sort();
        points.dedup();
        // Sweep the boundaries in order, keeping the tombstones that cover
        // the fragment starting at each.
        let (mut si, mut ei) = (0, 0);
        let mut active: BTreeSet<(Seq, usize)> = BTreeSet::new();
        let mut frags: Vec<(Bytes, Option<Bytes>, Seq)> = Vec::new();
        for (i, p) in points.iter().enumerate() {
            while ei < ends.len() && ends[ei].0 == *p {
                active.remove(&(rts[ends[ei].1].seq, ends[ei].1));
                ei += 1;
            }
            while si < starts.len() && starts[si].0 == *p {
                active.insert((rts[starts[si].1].seq, starts[si].1));
                si += 1;
            }
            let Some(&(seq, _)) = active.last() else { continue };
            let next = points.get(i + 1).map(|n| (*n).clone());
            match frags.last_mut() {
                Some((_, end, s)) if *s == seq && end.as_ref() == Some(*p) => *end = next,
                _ => frags.push(((*p).clone(), next, seq)),
            }
        }
        RangeDeletes { frags }
    }

    pub fn is_empty(&self) -> bool {
        self.frags.is_empty()
    }

    /// Returns the newest tombstone sequence number covering `key`, or 0 if uncovered.
    pub fn seq_at(&self, key: &[u8]) -> Seq {
        let i = self.frags.partition_point(|(s, _, _)| s.as_ref() <= key);
        match i.checked_sub(1).map(|i| &self.frags[i]) {
            Some((_, end, seq)) if end.as_ref().is_none_or(|e| key < e.as_ref()) => *seq,
            _ => 0,
        }
    }

    /// Returns whether any tombstone newer than `after` intersects `r`.
    pub fn changed_since(&self, r: &KeyRange, after: Seq) -> bool {
        self.frags.iter().any(|(s, e, seq)| *seq > after && KeyRange::half_open(s.clone(), e.clone()).intersects(r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::prop;
    use crate::util::rng::Rng;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn rt(start: &str, end: Option<&str>, seq: Seq) -> RangeTombstone {
        RangeTombstone { start: b(start), end: end.map(b), seq }
    }

    #[test]
    fn ranges_normalize_to_half_open() {
        let inc = |s: &str| Bound::Included(b(s));
        let exc = |s: &str| Bound::Excluded(b(s));
        let t = RangeTombstone::new(&KeyRange::new(exc("a"), inc("c")), 4).unwrap();
        assert_eq!(t, RangeTombstone { start: b("a\0"), end: Some(b("c\0")), seq: 4 });
        assert!(!t.contains_key(b"a") && t.contains_key(b"a\0") && t.contains_key(b"c") && !t.contains_key(b"c\0"));
        let all = RangeTombstone::new(&KeyRange::all(), 1).unwrap();
        assert!(all.contains_key(b"") && all.contains_key(b"\xff\xff") && all.end.is_none());
        assert_eq!(RangeTombstone::new(&KeyRange::half_open(b("b"), Some(b("b"))), 1), None);
        assert_eq!(RangeTombstone::new(&KeyRange::new(exc("b"), inc("b")), 1), None);
        assert_eq!(RangeTombstone::new(&KeyRange::prefix(b"ab"), 2).unwrap().range(), KeyRange::prefix(b"ab"));
    }

    #[test]
    fn containment_and_segments() {
        assert!(rt("b", Some("e"), 1).contains_range(&rt("c", Some("e"), 9)));
        assert!(!rt("b", Some("e"), 1).contains_range(&rt("c", None, 9)));
        assert!(rt("b", None, 1).contains_range(&rt("c", None, 9)));
        assert!(!rt("b", Some("e"), 1).contains_range(&rt("a", Some("c"), 9)));
        let seg = |min: &str, max: &str, min_seq, max_seq, applied| SegmentMeta {
            id: "0".repeat(32),
            level: 1,
            size: 1,
            entries: 1,
            tombstones: 0,
            min_key: b(min),
            max_key: b(max),
            min_seq,
            max_seq,
            deletes_applied: applied,
            expiring: 0,
            expires_max: 0,
        };
        let t = rt("c", Some("f"), 10);
        assert!(t.covers(&seg("c", "ez", 1, 9, 0)));
        assert!(!t.covers(&seg("c", "f", 1, 9, 0)), "f is past the end");
        assert!(!t.covers(&seg("c", "e", 1, 10, 0)), "the deleting commit's own writes survive");
        assert!(t.dirties(&seg("a", "c", 1, 20, 0)));
        assert!(!t.dirties(&seg("f", "g", 1, 9, 0)), "disjoint");
        assert!(!t.dirties(&seg("a", "z", 10, 20, 0)), "nothing old enough");
        assert!(!t.dirties(&seg("a", "z", 1, 20, 10)), "already applied");
    }

    #[test]
    fn prop_fragments_match_brute_force() {
        prop::check(
            "range-deletes",
            300,
            |r: &mut Rng| {
                (0..r.usize(6))
                    .map(|_| {
                        let a = prop::small_key(r, 2);
                        let e = prop::small_key(r, 2);
                        let end = (!r.chance(0.2)).then(|| Bytes::from(e));
                        (Bytes::from(a), end, 1 + r.below(5))
                    })
                    .filter_map(|(s, e, seq)| {
                        let t = RangeTombstone { start: s, end: e, seq };
                        (!t.is_empty()).then_some(t)
                    })
                    .collect::<Vec<_>>()
            },
            |v| prop::shrink_vec(v),
            |rts| {
                let d = RangeDeletes::new(rts);
                let mut r = Rng::new(rts.len() as u64);
                for _ in 0..200 {
                    let k = prop::small_key(&mut r, 3);
                    let want = rts.iter().filter(|t| t.contains_key(&k)).map(|t| t.seq).max().unwrap_or(0);
                    if d.seq_at(&k) != want {
                        return Err(format!("{k:?}: {} != {want}", d.seq_at(&k)));
                    }
                    let a = Bytes::from(prop::small_key(&mut r, 2));
                    let q = KeyRange::half_open(a.clone(), Some(Bytes::from(prop::small_key(&mut r, 2))));
                    let after = r.below(5);
                    let want = rts.iter().any(|t| t.seq > after && t.range().intersects(&q));
                    if d.changed_since(&q, after) != want {
                        return Err(format!("changed_since({q:?}, {after})"));
                    }
                }
                for w in d.frags.windows(2) {
                    if w[0].1.as_ref().is_none_or(|e| *e > w[1].0) {
                        return Err("fragments overlap".into());
                    }
                }
                Ok(())
            },
        );
    }

    #[test]
    fn codec() {
        for t in [rt("", None, 1), rt("a", Some("b"), u64::MAX)] {
            let mut w = Writer::new();
            t.encode(&mut w);
            let buf = w.into_inner();
            assert_eq!(RangeTombstone::decode(&mut Reader::new(&buf)).unwrap(), t);
        }
        let mut w = Writer::new();
        rt("b", Some("a"), 1).encode(&mut w);
        assert!(RangeTombstone::decode(&mut Reader::new(&w.into_inner())).is_err(), "empty ranges are corrupt");
    }
}
