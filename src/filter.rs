//! Filters (SPEC §4.7): boolean combinations of comparisons that select
//! table rows and stream records. This module owns the language: its JSON
//! form, its limits and what every comparison means. A layer supplies only
//! its operands, parsing what a comparison names ([`Operand`]) and resolving
//! it to a value that comparisons test ([`Target`]).
//!
//! A filter is exactly one of `{"and": [f, ...]}`, `{"or": [f, ...]}`,
//! `{"not": f}` or a comparison: the layer's operand keys, `op`, and a
//! `value` for every op but `exists`. A missing or null operand fails every
//! comparison, `ne` included, and only `exists` tells it apart.

use crate::error::{Error, Result};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Number, Value as Json};
use std::cmp::Ordering;
use std::hash::{DefaultHasher, Hash, Hasher};

/// The deepest a filter nests.
pub const DEPTH_MAX: usize = 16;
/// The most nodes (combinators and comparisons) a filter has.
pub const NODES_MAX: usize = 256;
/// The most values an `in` list holds.
pub const LIST_MAX: usize = 1000;
/// 2^127, past which no float is an i128.
const TWO_127: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;
/// Every comparison op, by name.
pub const OPS: [&str; 10] = ["eq", "ne", "lt", "le", "gt", "ge", "in", "prefix", "contains", "exists"];

/// A filter over operands `O`, whose comparisons take arguments `A`: JSON
/// as parsed, or whatever a layer binds them to.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter<O, A = Json> {
    And(Vec<Filter<O, A>>),
    Or(Vec<Filter<O, A>>),
    Not(Box<Filter<O, A>>),
    Cmp(O, Test<A>),
}

/// What a comparison asks of its operand's value.
#[derive(Debug, Clone, PartialEq)]
pub enum Test<A> {
    Eq(A),
    Ne(A),
    Lt(A),
    Le(A),
    Gt(A),
    Ge(A),
    In(List<A>),
    Prefix(A),
    Contains(A),
    Exists,
}

/// What a layer's comparisons name: a column, a field, a header.
pub trait Operand: Sized {
    /// Parses the operand from a comparison's keys other than `op` and
    /// `value`, removing the ones it reads. Keys left over are refused.
    fn parse(keys: &mut Map<String, Json>) -> Result<Self>;

    /// Refuses a test the operand cannot take.
    fn check(&self, _test: &Test<Json>) -> Result<()> {
        Ok(())
    }
}

/// A comparison argument.
pub trait Arg {
    /// The hash of the argument as equality sees it: see [`Target::eq_hash`].
    fn eq_hash(&self) -> u64;
}

/// A present operand value, tested against arguments of type `A`.
pub trait Target<A> {
    fn equals(&self, arg: &A) -> bool;
    /// A hash that the value shares with every argument it equals, by which
    /// `in` finds its candidates; `None` compares it with every argument.
    fn eq_hash(&self) -> Option<u64>;
    /// `None` when the two have no order between them, such as a number
    /// and a string.
    fn order(&self, arg: &A) -> Option<Ordering>;
    fn starts_with(&self, arg: &A) -> bool;
    fn contains(&self, arg: &A) -> bool;
}

impl<O, A> Filter<O, A> {
    /// Whether the filter holds, given each operand's value: `None` when it
    /// is missing or null.
    pub fn eval<T: Target<A>>(&self, value: &mut impl FnMut(&O) -> Option<T>) -> bool {
        match self {
            Filter::And(fs) => fs.iter().all(|f| f.eval(value)),
            Filter::Or(fs) => fs.iter().any(|f| f.eval(value)),
            Filter::Not(f) => !f.eval(value),
            Filter::Cmp(o, t) => value(o).is_some_and(|v| t.holds(&v)),
        }
    }

    /// The same filter with each comparison rewritten by `f`, as a layer
    /// resolves names and types its arguments once rather than per value.
    pub fn bind<P, B>(&self, f: &mut impl FnMut(&O, &Test<A>) -> Result<(P, Test<B>)>) -> Result<Filter<P, B>> {
        let all = |fs: &[Filter<O, A>], f: &mut _| fs.iter().map(|x| x.bind(f)).collect::<Result<Vec<_>>>();
        Ok(match self {
            Filter::And(fs) => Filter::And(all(fs, f)?),
            Filter::Or(fs) => Filter::Or(all(fs, f)?),
            Filter::Not(x) => Filter::Not(Box::new(x.bind(f)?)),
            Filter::Cmp(o, t) => {
                let (p, t) = f(o, t)?;
                Filter::Cmp(p, t)
            }
        })
    }
}

impl<O: Operand> Filter<O> {
    /// Parses a filter, refusing anything but exactly the grammar above.
    pub fn parse(v: Json) -> Result<Self> {
        Self::node(v, 0, &mut 0)
    }

    fn node(v: Json, depth: usize, nodes: &mut usize) -> Result<Self> {
        *nodes += 1;
        if depth > DEPTH_MAX || *nodes > NODES_MAX {
            return Err(Error::invalid(format!(
                "filters nest at most {DEPTH_MAX} deep and have at most {NODES_MAX} terms"
            )));
        }
        let Json::Object(mut o) = v else { return Err(Error::invalid("a filter is a JSON object")) };
        if o.keys().any(|k| matches!(k.as_str(), "and" | "or" | "not")) {
            if o.len() != 1 {
                return Err(not_one(&o));
            }
            let (k, v) = o.into_iter().next().expect("one key");
            let list = |v: Json, nodes: &mut usize| match v {
                Json::Array(fs) => fs.into_iter().map(|f| Self::node(f, depth + 1, nodes)).collect(),
                _ => Err(Error::invalid(format!("{k} takes an array of filters"))),
            };
            return Ok(match k.as_str() {
                "and" => Filter::And(list(v, nodes)?),
                "or" => Filter::Or(list(v, nodes)?),
                _ => Filter::Not(Box::new(Self::node(v, depth + 1, nodes)?)),
            });
        }
        let op = match o.remove("op") {
            Some(Json::String(op)) => op,
            Some(_) => return Err(Error::invalid("op is a string")),
            None => return Err(not_one(&o)),
        };
        let test = Test::parse(&op, o.remove("value"))?;
        let operand = O::parse(&mut o)?;
        if let Some(k) = o.keys().next() {
            return Err(Error::invalid(format!("unknown filter key {k:?}")));
        }
        operand.check(&test)?;
        Ok(Filter::Cmp(operand, test))
    }
}

fn not_one(o: &Map<String, Json>) -> Error {
    let keys: Vec<&str> = o.keys().map(String::as_str).collect();
    Error::invalid(format!(
        "a filter is exactly one of and, or, not or a comparison with op, not {{{}}}",
        keys.join(", ")
    ))
}

impl<'de, O: Operand> Deserialize<'de> for Filter<O> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Filter::parse(Json::deserialize(d)?).map_err(|e| serde::de::Error::custom(e.message()))
    }
}

impl Test<Json> {
    fn parse(op: &str, value: Option<Json>) -> Result<Self> {
        if !OPS.contains(&op) {
            return Err(Error::invalid(format!("unknown filter op {op:?}; ops are {}", OPS.join(", "))));
        }
        let fail = |m: &str| Err(Error::invalid(format!("{op} {m}")));
        let v = match (op, value) {
            ("exists", None) => return Ok(Test::Exists),
            ("exists", Some(_)) => return fail("takes no value"),
            (_, None | Some(Json::Null)) => return fail("needs a value; to match null, use not exists"),
            (_, Some(v)) => v,
        };
        let scalar = matches!(v, Json::Bool(_) | Json::Number(_) | Json::String(_));
        Ok(match op {
            "eq" => Test::Eq(v),
            "ne" => Test::Ne(v),
            "lt" | "le" | "gt" | "ge" if !scalar => return fail("takes a number, a string or a boolean"),
            "lt" => Test::Lt(v),
            "le" => Test::Le(v),
            "gt" => Test::Gt(v),
            "ge" => Test::Ge(v),
            "in" => match v {
                Json::Array(a) if a.len() <= LIST_MAX && !a.iter().any(Json::is_null) => Test::In(List::new(a)),
                _ => return fail(&format!("takes an array of at most {LIST_MAX} values, none null")),
            },
            "prefix" if v.is_string() => Test::Prefix(v),
            "prefix" => return fail("takes a string"),
            _ => Test::Contains(v),
        })
    }
}

impl<A> Test<A> {
    pub fn name(&self) -> &'static str {
        match self {
            Test::Eq(_) => "eq",
            Test::Ne(_) => "ne",
            Test::Lt(_) => "lt",
            Test::Le(_) => "le",
            Test::Gt(_) => "gt",
            Test::Ge(_) => "ge",
            Test::In(_) => "in",
            Test::Prefix(_) => "prefix",
            Test::Contains(_) => "contains",
            Test::Exists => "exists",
        }
    }

    /// The same test with every argument converted by `f`.
    pub fn try_map<B: Arg>(&self, mut f: impl FnMut(&A) -> Result<B>) -> Result<Test<B>> {
        Ok(match self {
            Test::Eq(a) => Test::Eq(f(a)?),
            Test::Ne(a) => Test::Ne(f(a)?),
            Test::Lt(a) => Test::Lt(f(a)?),
            Test::Le(a) => Test::Le(f(a)?),
            Test::Gt(a) => Test::Gt(f(a)?),
            Test::Ge(a) => Test::Ge(f(a)?),
            Test::In(l) => Test::In(List::new(l.items.iter().map(f).collect::<Result<_>>()?)),
            Test::Prefix(a) => Test::Prefix(f(a)?),
            Test::Contains(a) => Test::Contains(f(a)?),
            Test::Exists => Test::Exists,
        })
    }

    /// Whether the test holds for a present value.
    pub fn holds<T: Target<A>>(&self, t: &T) -> bool {
        use Ordering::*;
        match self {
            Test::Eq(a) => t.equals(a),
            Test::Ne(a) => !t.equals(a),
            Test::Lt(a) => t.order(a) == Some(Less),
            Test::Le(a) => matches!(t.order(a), Some(Less | Equal)),
            Test::Gt(a) => t.order(a) == Some(Greater),
            Test::Ge(a) => matches!(t.order(a), Some(Greater | Equal)),
            Test::In(l) => l.any_equals(t),
            Test::Prefix(a) => t.starts_with(a),
            Test::Contains(a) => t.contains(a),
            Test::Exists => true,
        }
    }
}

/// The arguments of an `in` test, indexed by hash so that testing a value
/// costs about the same however long the list is.
#[derive(Debug, Clone)]
pub struct List<A> {
    items: Vec<A>,
    /// The hash and index of every item, sorted.
    by_hash: Vec<(u64, u32)>,
}

impl<A: Arg> List<A> {
    pub fn new(items: Vec<A>) -> List<A> {
        let mut by_hash: Vec<(u64, u32)> = items.iter().zip(0..).map(|(a, i)| (a.eq_hash(), i)).collect();
        by_hash.sort_unstable();
        List { items, by_hash }
    }
}

impl<A> List<A> {
    pub fn items(&self) -> &[A] {
        &self.items
    }

    /// Whether `t` equals some item.
    pub fn any_equals<T: Target<A>>(&self, t: &T) -> bool {
        let Some(h) = t.eq_hash() else { return self.items.iter().any(|a| t.equals(a)) };
        let from = self.by_hash.partition_point(|(x, _)| *x < h);
        self.by_hash[from..].iter().take_while(|(x, _)| *x == h).any(|(_, i)| t.equals(&self.items[*i as usize]))
    }
}

impl<A: PartialEq> PartialEq for List<A> {
    fn eq(&self, o: &List<A>) -> bool {
        self.items == o.items
    }
}

/// A value as equality sees it, to hash: numbers by their exact values,
/// whether written as integers or floats; strings and byte strings by their
/// bytes; and JSON as JSON compares.
#[derive(Clone, Copy, Debug)]
pub enum Plain<'a> {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(&'a str),
    Bytes(&'a [u8]),
    Json(&'a Json),
}

impl Plain<'_> {
    pub fn eq_hash(self) -> u64 {
        let mut h = DefaultHasher::new();
        self.feed(&mut h);
        h.finish()
    }

    fn feed(self, h: &mut DefaultHasher) {
        match self {
            Plain::Null => 0u8.hash(h),
            Plain::Bool(b) => (1u8, b).hash(h),
            Plain::Int(i) => (2u8, i).hash(h),
            // An integral float hashes as the integer it equals; -0.0 as 0.
            Plain::Float(f) if f.fract() == 0.0 && f.abs() < TWO_127 => (2u8, f as i128).hash(h),
            Plain::Float(f) => (3u8, f.to_bits()).hash(h),
            Plain::Str(s) => (4u8, s).hash(h),
            Plain::Bytes(b) => (5u8, b).hash(h),
            Plain::Json(Json::Null) => Plain::Null.feed(h),
            Plain::Json(Json::Bool(b)) => Plain::Bool(*b).feed(h),
            Plain::Json(Json::Number(n)) => match n.as_i64().map(i128::from).or_else(|| n.as_u64().map(i128::from)) {
                Some(i) => Plain::Int(i).feed(h),
                None => Plain::Float(n.as_f64().unwrap_or(f64::NAN)).feed(h),
            },
            Plain::Json(Json::String(s)) => Plain::Str(s).feed(h),
            Plain::Json(Json::Array(a)) => {
                (6u8, a.len()).hash(h);
                a.iter().for_each(|x| Plain::Json(x).feed(h));
            }
            Plain::Json(Json::Object(o)) => {
                // Equal objects may list their keys in different orders.
                let entries = o.iter().fold(0u64, |sum, (k, v)| {
                    let mut e = DefaultHasher::new();
                    k.hash(&mut e);
                    Plain::Json(v).feed(&mut e);
                    sum.wrapping_add(e.finish())
                });
                (7u8, o.len(), entries).hash(h);
            }
        }
    }
}

impl Arg for Json {
    fn eq_hash(&self) -> u64 {
        Plain::Json(self).eq_hash()
    }
}

/// JSON values compare as JSON: numbers by value (`1` equals `1.0`),
/// strings by their bytes, booleans with false first, and arrays and
/// objects only for equality. A string contains its substrings and an array
/// its elements.
impl Target<Json> for &Json {
    fn equals(&self, arg: &Json) -> bool {
        json_eq(self, arg)
    }

    fn eq_hash(&self) -> Option<u64> {
        Some(Plain::Json(self).eq_hash())
    }

    fn order(&self, arg: &Json) -> Option<Ordering> {
        match (self, arg) {
            (Json::Number(x), Json::Number(y)) => number_order(x, y),
            (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
            (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
            _ => None,
        }
    }

    fn starts_with(&self, arg: &Json) -> bool {
        matches!((self, arg), (Json::String(s), Json::String(p)) if s.starts_with(p.as_str()))
    }

    fn contains(&self, arg: &Json) -> bool {
        match (self, arg) {
            (Json::String(s), Json::String(p)) => s.contains(p.as_str()),
            (Json::Array(a), v) => a.iter().any(|x| json_eq(x, v)),
            _ => false,
        }
    }
}

/// A string compares as the JSON string it would be.
impl Target<Json> for &str {
    fn equals(&self, arg: &Json) -> bool {
        arg.as_str() == Some(*self)
    }

    fn eq_hash(&self) -> Option<u64> {
        Some(Plain::Str(self).eq_hash())
    }

    fn order(&self, arg: &Json) -> Option<Ordering> {
        Some((*self).cmp(arg.as_str()?))
    }

    fn starts_with(&self, arg: &Json) -> bool {
        arg.as_str().is_some_and(|p| str::starts_with(self, p))
    }

    fn contains(&self, arg: &Json) -> bool {
        arg.as_str().is_some_and(|p| str::contains(self, p))
    }
}

fn json_eq(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Number(x), Json::Number(y)) => number_order(x, y) == Some(Ordering::Equal),
        (Json::Array(x), Json::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(x, y)| json_eq(x, y)),
        (Json::Object(x), Json::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        _ => a == b,
    }
}

/// Compares two JSON numbers by their exact values.
fn number_order(x: &Number, y: &Number) -> Option<Ordering> {
    let int = |n: &Number| n.as_i64().map(i128::from).or_else(|| n.as_u64().map(i128::from));
    match (int(x), int(y)) {
        (Some(a), Some(b)) => Some(a.cmp(&b)),
        (Some(a), None) => int_float_order(a, y.as_f64()?),
        (None, Some(b)) => int_float_order(b, x.as_f64()?).map(Ordering::reverse),
        (None, None) => x.as_f64()?.partial_cmp(&y.as_f64()?),
    }
}

/// Compares integer `i` with float `f` exactly, where converting either to
/// the other's type would round.
pub(crate) fn int_float_order(i: i128, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f >= TWO_127 {
        return Some(Ordering::Less);
    }
    if f < -TWO_127 {
        return Some(Ordering::Greater);
    }
    let t = f.trunc();
    Some(match i.cmp(&(t as i128)) {
        Ordering::Equal if f > t => Ordering::Less,
        Ordering::Equal if f < t => Ordering::Greater,
        o => o,
    })
}

/// Whether `needle` occurs in `hay`.
pub fn contains_bytes(hay: &[u8], needle: &[u8]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Numbers near the limits of f64 and i64 compare as their exact values:
    /// the order is total and agrees with i128 arithmetic where both are
    /// integers.
    #[test]
    fn numbers_compare_exactly() {
        let mut r = crate::util::rng::Rng::new(5);
        let mut nums: Vec<Number> = Vec::new();
        for _ in 0..400 {
            let base = [0i64, 1 << 53, 1 << 60, i64::MAX - 1024, -(1 << 53)][r.usize(5)];
            let i = base.wrapping_add(r.below(64) as i64 - 32);
            nums.push(Number::from(i));
            nums.push(Number::from_f64(i as f64 + [0.0, 0.5, -0.25][r.usize(3)]).unwrap());
        }
        nums.push(Number::from(u64::MAX));
        nums.push(Number::from_f64(18_446_744_073_709_551_616.0).unwrap());
        let ord = |a: &Number, b: &Number| number_order(a, b).unwrap();
        for a in &nums {
            for b in nums.iter().take(120) {
                assert_eq!(ord(a, b), ord(b, a).reverse(), "{a} vs {b}");
                if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
                    assert_eq!(ord(a, b), x.cmp(&y));
                }
            }
        }
        let mut sorted = nums.clone();
        sorted.sort_by(ord);
        for w in sorted.windows(3) {
            assert_ne!(ord(&w[0], &w[2]), Ordering::Greater, "{} {} {}", w[0], w[1], w[2]);
        }
        let big = Number::from(1_152_921_504_606_846_977i64);
        assert_eq!(ord(&big, &Number::from_f64(1_152_921_504_606_846_976.0).unwrap()), Ordering::Greater);
        assert!(!json_eq(&json!([1_152_921_504_606_846_976.0]), &json!([1_152_921_504_606_846_977i64])));
    }

    /// Values that compare equal hash alike, so an `in` list finds exactly
    /// the values that comparing with each of its items would.
    #[test]
    fn lists_find_what_equality_finds() {
        let mut r = crate::util::rng::Rng::new(7);
        let scalar = |r: &mut crate::util::rng::Rng| -> Json {
            let i = [0i64, 1, -1, 1 << 53, 1 << 60, i64::MAX, i64::MIN][r.usize(7)].wrapping_add(r.below(3) as i64 - 1);
            match r.usize(8) {
                0 => json!(i),
                1 => json!(i as f64),
                2 => json!([0.0, -0.0, 0.5, 1e308, 5e-324, 18_446_744_073_709_551_616.0][r.usize(6)]),
                3 => json!(u64::MAX - r.below(2)),
                4 => json!(["", "a", "1", "0"][r.usize(4)]),
                5 => json!(r.chance(0.5)),
                6 => json!(i as f64 + 0.5),
                _ => json!(i % 3),
            }
        };
        let value = |r: &mut crate::util::rng::Rng| -> Json {
            match r.usize(5) {
                0 => json!([scalar(r), scalar(r)]),
                1 => json!({"a": scalar(r), "b": [scalar(r)]}),
                2 => json!({"b": [scalar(r)], "a": scalar(r)}),
                _ => scalar(r),
            }
        };
        let vals: Vec<Json> = (0..600).map(|_| value(&mut r)).collect();
        for a in &vals {
            for b in &vals {
                if json_eq(a, b) {
                    assert_eq!(Arg::eq_hash(a), Arg::eq_hash(b), "{a} and {b} are equal");
                }
            }
        }
        for _ in 0..200 {
            let items: Vec<Json> = (0..r.usize(50)).map(|_| vals[r.usize(vals.len())].clone()).collect();
            let list = List::new(items.clone());
            for v in vals.iter().take(100) {
                assert_eq!(list.any_equals(&v), items.iter().any(|a| json_eq(v, a)), "{v} in {items:?}");
                if let Some(s) = v.as_str() {
                    assert_eq!(list.any_equals(&s), items.iter().any(|a| json_eq(v, a)), "{v} in {items:?}");
                }
            }
        }
    }

    /// Comparisons name one JSON value, `v`.
    #[derive(Debug, PartialEq)]
    struct V;

    impl Operand for V {
        fn parse(keys: &mut Map<String, Json>) -> Result<V> {
            match keys.remove("v") {
                Some(Json::Bool(true)) => Ok(V),
                _ => Err(Error::invalid("v: true")),
            }
        }
    }

    fn parse(v: Json) -> Result<Filter<V>> {
        serde_json::from_value(v).map_err(|e| Error::invalid(e.to_string()))
    }

    fn holds(f: Json, v: Option<Json>) -> bool {
        parse(f.clone()).unwrap_or_else(|e| panic!("{f}: {e}")).eval(&mut |_: &V| v.as_ref())
    }

    #[test]
    fn comparisons() {
        let doc = json!({"n": 3});
        for (yes, f, v) in [
            (true, json!({"v": true, "op": "eq", "value": 3.0}), json!(3)),
            (true, json!({"v": true, "op": "eq", "value": [1.0, {"a": 2}]}), json!([1, {"a": 2.0}])),
            (true, json!({"v": true, "op": "ne", "value": "3"}), json!(3)),
            (true, json!({"v": true, "op": "lt", "value": 4}), json!(3)),
            (false, json!({"v": true, "op": "lt", "value": "4"}), json!(3)),
            (true, json!({"v": true, "op": "ge", "value": u64::MAX}), json!(u64::MAX)),
            (true, json!({"v": true, "op": "gt", "value": false}), json!(true)),
            (true, json!({"v": true, "op": "le", "value": "b"}), json!("ab")),
            (true, json!({"v": true, "op": "in", "value": [1, 3]}), json!(3)),
            (false, json!({"v": true, "op": "in", "value": []}), json!(3)),
            (true, json!({"v": true, "op": "prefix", "value": "ti"}), json!("time")),
            (false, json!({"v": true, "op": "prefix", "value": "ti"}), json!(["time"])),
            (true, json!({"v": true, "op": "contains", "value": "im"}), json!("time")),
            (true, json!({"v": true, "op": "contains", "value": 2.0}), json!([1, 2])),
            (false, json!({"v": true, "op": "contains", "value": "a"}), json!({"a": 1})),
            (true, json!({"v": true, "op": "eq", "value": {"n": 3}}), doc.clone()),
            (true, json!({"v": true, "op": "exists"}), doc),
            (true, json!({"and": []}), json!(0)),
            (false, json!({"or": []}), json!(0)),
            (false, json!({"not": {"v": true, "op": "exists"}}), json!(0)),
        ] {
            assert_eq!(holds(f.clone(), Some(v.clone())), yes, "{f} on {v}");
        }
    }

    #[test]
    fn a_missing_operand_fails_every_comparison() {
        for op in ["eq", "ne", "lt", "le", "gt", "ge", "prefix", "contains"] {
            assert!(!holds(json!({"v": true, "op": op, "value": "x"}), None), "{op}");
        }
        assert!(!holds(json!({"v": true, "op": "in", "value": ["x"]}), None));
        assert!(!holds(json!({"v": true, "op": "exists"}), None));
        assert!(holds(json!({"not": {"v": true, "op": "eq", "value": 1}}), None));
    }

    #[test]
    fn a_node_is_exactly_one_thing() {
        let a = json!({"v": true, "op": "exists"});
        for bad in [
            json!({"and": [a], "or": [a]}),
            json!({"not": a, "v": true, "op": "exists"}),
            json!({"and": [], "bogus": 1}),
            json!({"v": true, "op": "exists", "bogus": 1}),
            json!({"bogus": 1}),
            json!({}),
            json!([a]),
            json!({"and": a}),
            json!({"not": [a]}),
        ] {
            let e = parse(bad.clone()).expect_err(&bad.to_string());
            assert_eq!(e.code(), "invalid_argument", "{bad}");
        }
    }

    #[test]
    fn values_fit_their_op() {
        for bad in [
            json!({"v": true, "op": "between", "value": 1}),
            json!({"v": true, "op": 1, "value": 1}),
            json!({"v": true, "op": "eq"}),
            json!({"v": true, "op": "eq", "value": null}),
            json!({"v": true, "op": "exists", "value": true}),
            json!({"v": true, "op": "in", "value": 3}),
            json!({"v": true, "op": "in", "value": [1, null]}),
            json!({"v": true, "op": "in", "value": vec![1; LIST_MAX + 1]}),
            json!({"v": true, "op": "prefix", "value": 3}),
            json!({"v": true, "op": "lt", "value": [1]}),
            json!({"v": true, "op": "ge", "value": {}}),
            json!({"op": "eq", "value": 1}),
        ] {
            assert!(parse(bad.clone()).is_err(), "{bad}");
        }
        assert!(parse(json!({"v": true, "op": "in", "value": vec![1; LIST_MAX]})).is_ok());
    }

    #[test]
    fn size_is_bounded() {
        let leaf = json!({"v": true, "op": "exists"});
        let nest = |n: usize, wrap: fn(Json) -> Json| (0..n).fold(leaf.clone(), |f, _| wrap(f));
        assert!(parse(nest(DEPTH_MAX, |f| json!({"not": f}))).is_ok());
        assert!(parse(nest(DEPTH_MAX + 1, |f| json!({"not": f}))).is_err());
        assert!(parse(nest(DEPTH_MAX, |f| json!({"and": [f]}))).is_ok());
        assert!(parse(nest(DEPTH_MAX + 1, |f| json!({"or": [f]}))).is_err());
        assert!(parse(json!({"or": vec![leaf.clone(); NODES_MAX - 1]})).is_ok());
        assert!(parse(json!({"or": vec![leaf; NODES_MAX]})).is_err());
    }
}
