//! Paginated prefix scans over catalog entries keyed by `space ++ name` (SPEC §10).

use crate::engine::{KeyRange, ReadValue, Txn};
use crate::error::{Error, Result};
use crate::util::b64;
use bytes::Bytes;
use std::ops::Bound;

/// A page of `(name, entry)` pairs and the cursor for the next page.
pub type NamesPage = (Vec<(String, ReadValue)>, Option<String>);

/// Scans up to `limit` entries with keys `space ++ name` where `name` starts
/// with `prefix`, resuming after `cursor` if present.
pub async fn names_page(
    txn: &mut Txn,
    space: &[u8],
    prefix: &str,
    limit: usize,
    cursor: Option<&str>,
) -> Result<NamesPage> {
    let Some(range) = names_after(space, prefix, cursor)? else { return Ok((Vec::new(), None)) };
    let mut items = txn.scan(&range, false, limit.saturating_add(1)).await?;
    let more = items.len() > limit;
    items.truncate(limit);
    let page: Vec<(String, ReadValue)> =
        items.into_iter().map(|it| (String::from_utf8_lossy(&it.key[space.len()..]).into_owned(), it)).collect();
    let next = page.last().filter(|_| more).map(|(n, _)| b64::encode_url(n.as_bytes()));
    Ok((page, next))
}

fn names_after(space: &[u8], prefix: &str, cursor: Option<&str>) -> Result<Option<KeyRange>> {
    let key = |name: &[u8]| Bytes::from([space, name].concat());
    let range = KeyRange::prefix(&key(prefix.as_bytes()));
    let Some(c) = cursor else { return Ok(Some(range)) };
    let after = key(&b64::decode_url(c).ok_or_else(|| Error::invalid("bad cursor"))?);
    Ok(match range.contains(&after) {
        true => Some(KeyRange::new(Bound::Excluded(after), range.end)),
        false if after < key(prefix.as_bytes()) => Some(range),
        false => None,
    })
}
