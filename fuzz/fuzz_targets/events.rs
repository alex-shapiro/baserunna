#![no_main]
//! Event part references, table state, masks, binds, part metadata, column
//! indexes and blocks, aggregate states and queries must decode or parse
//! without panicking, and what decodes must survive a round trip.
use baserunna::engine::events::{Bind, PartRef, Runs, TableState, decode_mask_object, encode_mask_object, ref_key};
use baserunna::events::agg::State;
use baserunna::events::part::{ColIndex, ColMeta, ColRef, PartMeta, decode_block};
use baserunna::events::query::Query;
use baserunna::events::vector::Phys;
use baserunna::util::codec::Reader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let b = bytes::Bytes::copy_from_slice(data);
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
    let _ = PartMeta::decode(&b, 3);
    let mut part = PartMeta {
        rows: 3,
        granules: vec![2, 1],
        starts: vec![0, 2, 3],
        sort_ids: vec![],
        pk: vec![bytes::Bytes::new(); 3],
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
    for p in [Phys::Int, Phys::Float, Phys::Bool, Phys::Str] {
        part.cols[0].phys = p;
        let _ = ColIndex::decode(&b, &part, &part.cols[0]);
        let _ = decode_block(data, p, Some(&std::sync::Arc::new(vec!["a".into(), "b".into()])), 3);
    }
    let _ = State::decode(&mut Reader::new(data));
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = Query::parse(v);
    }
});
