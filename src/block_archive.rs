use crate::Result;
use async_trait::async_trait;
use bitcoinsv::bitcoin::{Block, BlockHash, BlockHeader};
use bytes::Bytes;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_stream::Stream;

/// The BlockArchive stores blocks, where a block is a BlockHeader and the transactions
/// that are required to validate the block.
///
/// The BlockArchive has very little knowledge of the structure of block, it only knows how to
/// store and retrieve blocks.
#[async_trait]
pub trait BlockArchive {
    /// Get a reader to a block from the archive.
    ///
    /// Returns a reader for the encoded block.
    ///
    /// This function does not do any checking of the block, it merely returns a reader for the
    /// bytes in the block.
    async fn get_block(&self, block_hash: &BlockHash) -> Result<Box<dyn AsyncRead + Unpin + Send>>;

    /// Get a block from the archive, loading it completely into memory.
    async fn get_block_full(&self, block_hash: &BlockHash) -> Result<Block>;

    /// Check if a block exists in the archive.
    async fn block_exists(&self, block_hash: &BlockHash) -> Result<bool>;

    /// Store a block in the archive.
    ///
    /// Expects a reader for the encoded block.
    ///
    /// This function does not do any checking of the block, it stores the bytes of the block as is.
    async fn store_block(
        &self,
        block_hash: &BlockHash,
        block: &mut Box<dyn AsyncRead + Unpin + Send>,
    ) -> Result<()>;

    /// Store a full block in the archive.
    async fn store_block_full(&self, block: &Block) -> Result<()>;

    /// Get the size of a block in the archive.
    async fn block_size(&self, block_hash: &BlockHash) -> Result<usize>;

    /// Get the number of transactions in a block.
    async fn block_tx_count(&self, block_hash: &BlockHash) -> Result<i64>;

    /// Get the header of a block in the archive.
    async fn block_header(&self, block_hash: &BlockHash) -> Result<BlockHeader>;

    /// Get a specific number of bytes from an offset in the block.
    ///
    /// Returns a vector of bytes.
    ///
    /// Can be used to retrieve a transaction if the location is known.
    async fn get_bytes_from_block(
        &self,
        block_hash: &BlockHash,
        offset: u64,
        length: u64,
    ) -> Result<Bytes>;

    /// Get a list of all the blocks in the archive.
    ///
    /// It returns a stream of block hashes.
    ///
    /// Example code:
    ///     let mut results = archive.block_list().await.unwrap();
    ///     while let Some(block_hash) = results.next().await {
    ///       println!("{}", block_hash);
    ///     }
    async fn block_list(&mut self) -> Result<Pin<Box<dyn BlockHashListStream<Item = BlockHash>>>>;
}

/// A stream of block hashes, returned by [BlockArchive::block_list].
///
/// Implemented as a trait for future extensibility.
pub trait BlockHashListStream: Stream<Item = BlockHash> {}

/// An implementation of the [BlockHashListStream] trait.
///
/// Built for the SimpleFileBasedBlockArchive but expected to be useful elsewhere.
/// It expects a background task to be created which sends block hashes to a channel. This stream
/// reads the block hashes from the channel.
pub struct BlockHashListStreamFromChannel {
    // The receiver to which the background task sends block hashes.
    receiver: Receiver<BlockHash>,
    // Handle to the background task that reads the block hashes.
    handle: JoinHandle<Result<()>>,
}

impl BlockHashListStreamFromChannel {
    /// Create a new BlockHashListStreamFromChannel, with a receiving end of a channel and a handle
    /// to the background process. The handle is used to close the background task when the stream
    /// is dropped.
    pub fn new(
        receiver: Receiver<BlockHash>,
        handle: JoinHandle<Result<()>>,
    ) -> BlockHashListStreamFromChannel {
        BlockHashListStreamFromChannel { receiver, handle }
    }
}

impl Stream for BlockHashListStreamFromChannel {
    type Item = BlockHash;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}

impl BlockHashListStream for BlockHashListStreamFromChannel {}

impl Drop for BlockHashListStreamFromChannel {
    // close the handle to the background task when the stream is dropped
    fn drop(&mut self) {
        if self.handle.is_finished() {
            return;
        }
        self.handle.abort();
    }
}
