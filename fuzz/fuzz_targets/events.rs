#![no_main]
//! Event part references, table state, masks, binds, part streams (their
//! metadata, column indexes and blocks), aggregate states and queries must
//! decode or parse without panicking, and what decodes must survive a round
//! trip.
use baserunna::engine::events::{Bind, PartRef, Runs, TableState, decode_mask_object, encode_mask_object, ref_key};
use baserunna::engine::segment::{Compression, frame};
use baserunna::events::agg::State;
use baserunna::events::part::{ColIndex, ColMeta, ColRef, PartMeta, decode_block, inspect};
use baserunna::events::query::Query;
use baserunna::events::vector::Phys;
use baserunna::util::codec::Reader;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

/// A part stream built from `data`: a row count byte, then sections of a
/// little-endian `u16` length and that many bytes, each framed so that its
/// checksum holds; the last section is the metadata.
fn part(data: &[u8]) -> Option<(Bytes, PartRef)> {
    let (&rows, mut rest) = data.split_first()?;
    let mut stream = Vec::new();
    let mut meta = (0, 0);
    while let [a, b, tail @ ..] = rest {
        let n = (u16::from_le_bytes([*a, *b]) as usize).min(tail.len());
        let off = stream.len() as u64;
        frame(&tail[..n], Compression::None, &mut stream);
        meta = (off, stream.len() as u64 - off);
        rest = &tail[n..];
    }
    let r = PartRef {
        id: "0123456789abcdef0123456789abcdef".into(),
        table: 1,
        partition: 0,
        chunk_bytes: 1 << 20,
        size: stream.len() as u64,
        raw_size: rows as u64,
        rows: rows as u64,
        level: 0,
        meta_off: meta.0,
        meta_len: meta.1,
        min_time: 0,
        max_time: 0,
        arrived_ms: 0,
        version: None,
        min_version: 0,
        max_version: 0,
        deleted: 0,
        mask: None,
    };
    Some((Bytes::from(stream), r))
}

fuzz_target!(|data: &[u8]| {
    let b = Bytes::copy_from_slice(data);
    let key = ref_key(1, 2, "0123456789abcdef0123456789abcdef");
    if let Ok(r) = PartRef::decode(&key, data) {
        assert_eq!(PartRef::decode(&key, &r.encode()), Ok(r));
    }
    if let Ok(st) = TableState::decode(data) {
        assert_eq!(TableState::decode(&st.encode()), Ok(st));
    }
    if let Ok(m) = decode_mask_object(data) {
        assert_eq!(decode_mask_object(&encode_mask_object(&m)), Ok(m));
    }
    if let Ok(r) = Runs::decode(&mut Reader::new(data)) {
        assert_eq!(Runs::decode(&mut Reader::new(&r.to_bytes())), Ok(r));
    }
    let _ = Bind::decode(data);
    if let Some((stream, r)) = part(data) {
        let _ = inspect(&stream, &r);
    }
    let mut meta = PartMeta {
        rows: 3,
        granules: vec![2, 1],
        starts: vec![0, 2, 3],
        sort_ids: vec![],
        pk: vec![Bytes::new(); 3],
        cols: vec![ColMeta {
            cref: ColRef::Ordinal,
            phys: Phys::Int,
            json: false,
            index_off: 1 << 20,
            index_len: data.len() as u64,
            data_off: 0,
            data_len: 1 << 20,
            nonnull: 3,
        }],
        shared: Default::default(),
        partial: Default::default(),
    };
    let _ = PartMeta::decode(&b, meta.rows);
    for p in [Phys::Int, Phys::Float, Phys::Bool, Phys::Str] {
        meta.cols[0].phys = p;
        let _ = ColIndex::decode(&b, &meta, &meta.cols[0]);
        let _ = decode_block(data, p, Some(&std::sync::Arc::new(vec!["a".into(), "b".into()])), 2);
    }
    let _ = State::decode(&mut Reader::new(data));
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = Query::parse(v);
    }
});
