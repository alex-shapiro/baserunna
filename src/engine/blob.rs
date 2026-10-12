//! Out-of-band blobs: immutable byte sequences stored as fixed-size chunk
//! objects under `dbs/{db}/blob/{id}/`.

use super::types::{BlobRef, make_blob_id};
use super::{Shared, blob_chunk_key};
use crate::error::{Error, Result};
use crate::store::{GetOpts, PutMode};
use crate::util::Sha256;
use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;

pub struct BlobWriter {
    shared: Arc<Shared>,
    db: String,
    id: String,
    buf: Vec<u8>,
    next_chunk: u64,
    size: u64,
    sha: Sha256,
    content_type: String,
}

impl BlobWriter {
    pub fn new(shared: Arc<Shared>, db: &str, content_type: &str) -> BlobWriter {
        let id = make_blob_id(shared.env.now_ms(), &shared.env.random_bytes(10));
        BlobWriter {
            buf: Vec::with_capacity(shared.cfg.blob_chunk_size.min(1 << 20)),
            shared,
            db: db.to_string(),
            id,
            next_chunk: 0,
            size: 0,
            sha: Sha256::new(),
            content_type: content_type.to_string(),
        }
    }

    async fn flush_chunk(&mut self, data: Bytes) -> Result<()> {
        let key = blob_chunk_key(&self.db, &self.id, self.next_chunk);
        self.shared.retry.run(|| self.shared.store.put(&key, data.clone(), PutMode::Overwrite)).await?;
        self.next_chunk += 1;
        Ok(())
    }

    pub async fn write(&mut self, mut data: &[u8]) -> Result<()> {
        self.size += data.len() as u64;
        if self.size > self.shared.cfg.blob_max {
            return Err(Error::TooLarge(format!("blob exceeds {} bytes", self.shared.cfg.blob_max)));
        }
        self.sha.update(data);
        let cs = self.shared.cfg.blob_chunk_size;
        while !data.is_empty() {
            let take = (cs - self.buf.len()).min(data.len());
            self.buf.extend_from_slice(&data[..take]);
            data = &data[take..];
            if self.buf.len() == cs {
                let chunk = Bytes::from(std::mem::replace(&mut self.buf, Vec::with_capacity(cs.min(1 << 20))));
                self.flush_chunk(chunk).await?;
            }
        }
        Ok(())
    }

    pub async fn finish(mut self) -> Result<BlobRef> {
        if !self.buf.is_empty() {
            let chunk = Bytes::from(std::mem::take(&mut self.buf));
            self.flush_chunk(chunk).await?;
        }
        let r = BlobRef { id: self.id, size: self.size, sha256: self.sha.finish(), content_type: self.content_type };
        // Descriptor object so a staged blob can be bound by id later.
        let mut w = crate::util::codec::Writer::new();
        r.encode(&mut w);
        let meta = Bytes::from(w.into_inner());
        let key = blob_meta_key(&self.db, &r.id);
        self.shared.retry.run(|| self.shared.store.put(&key, meta.clone(), PutMode::Overwrite)).await?;
        Ok(r)
    }
}

pub fn blob_meta_key(db: &str, id: &str) -> String {
    format!("{}meta", super::blob_prefix(db, id))
}

/// Load the descriptor of a staged (or committed) blob by id.
pub async fn load_staged(shared: &Shared, db: &str, id: &str) -> Result<BlobRef> {
    if !super::types::valid_object_id(id) {
        return Err(Error::invalid(format!("invalid blob id {id:?}")));
    }
    let key = blob_meta_key(db, id);
    let g = shared.retry.run(|| shared.store.get(&key, GetOpts::default())).await.map_err(|e| match e {
        crate::store::StoreError::NotFound => Error::not_found(format!("blob {id} not found")),
        other => other.into(),
    })?;
    let r = BlobRef::decode(&mut crate::util::codec::Reader::new(&g.data))?;
    if r.id != id {
        return Err(Error::internal("blob descriptor mismatch"));
    }
    Ok(r)
}

/// Write a whole in-memory value as a blob.
pub async fn write_blob(shared: &Arc<Shared>, db: &str, data: &[u8], content_type: &str) -> Result<BlobRef> {
    let mut w = BlobWriter::new(shared.clone(), db, content_type);
    w.write(data).await?;
    w.finish().await
}

/// The chunk reads needed to serve `range` of a blob: (chunk index, range
/// within the chunk).
pub fn chunk_plan(chunk_size: u64, range: Range<u64>) -> Vec<(u64, Range<u64>)> {
    let mut out = Vec::new();
    let mut pos = range.start;
    while pos < range.end {
        let ci = pos / chunk_size;
        let cstart = ci * chunk_size;
        let end = range.end.min(cstart + chunk_size);
        out.push((ci, pos - cstart..end - cstart));
        pos = end;
    }
    out
}

/// Read one planned chunk piece.
pub async fn read_chunk(shared: &Shared, db: &str, blob: &BlobRef, ci: u64, r: Range<u64>) -> Result<Bytes> {
    let key = blob_chunk_key(db, &blob.id, ci);
    let g = shared
        .retry
        .run(|| shared.store.get(&key, GetOpts { range: Some(r.clone()), ..Default::default() }))
        .await
        .map_err(|e| match e {
            // The snapshot outlived the blob: it was dereferenced and then
            // collected after `gc_grace`. A retry reads a newer snapshot.
            crate::store::StoreError::NotFound => {
                Error::unavailable(format!("blob {} was garbage collected during the read; retry", blob.id))
            }
            other => other.into(),
        })?;
    if g.data.len() as u64 != r.end - r.start {
        return Err(Error::internal("short blob chunk read"));
    }
    Ok(g.data)
}

/// Read a byte range of a blob into memory (for small blobs / tests).
pub async fn read_range(shared: &Shared, db: &str, blob: &BlobRef, range: Range<u64>) -> Result<Bytes> {
    if range.start > range.end || range.end > blob.size {
        return Err(Error::invalid("blob range out of bounds"));
    }
    let mut out = Vec::with_capacity((range.end - range.start) as usize);
    for (ci, r) in chunk_plan(shared.cfg.blob_chunk_size as u64, range) {
        out.extend_from_slice(&read_chunk(shared, db, blob, ci, r).await?);
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan() {
        assert_eq!(chunk_plan(10, 0..0), vec![]);
        assert_eq!(chunk_plan(10, 0..10), vec![(0, 0..10)]);
        assert_eq!(chunk_plan(10, 5..25), vec![(0, 5..10), (1, 0..10), (2, 0..5)]);
        assert_eq!(chunk_plan(10, 20..21), vec![(2, 0..1)]);
    }
}
