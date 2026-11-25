//! Blob and snapshot storage: a local folder or S3.
//!
//! Placeholder: Task 6 gives the trait its full contract and the local implementation; Task 17 adds S3.

use async_trait::async_trait;

#[async_trait]
pub trait Blobs: Send + Sync + 'static {
    /// Answers when the storage does (`GET /api/ready`).
    async fn health(&self) -> anyhow::Result<()>;
}

/// Storage that keeps nothing and is always healthy.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullBlobs;

#[async_trait]
impl Blobs for NullBlobs {
    async fn health(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
