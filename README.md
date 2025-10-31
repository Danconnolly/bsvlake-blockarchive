# BSVLake BlockArchive

A high-performance block storage library for Bitcoin SV, providing efficient storage and retrieval of blockchain blocks.

## Features

- **Async/await support** - Built on Tokio for high-performance async I/O
- **Simple file-based storage** - Blocks stored in a hierarchical directory structure
- **Streaming support** - Stream block hashes without loading everything into memory
- **Flexible API** - Support for both streaming and full block loading
- **Efficient storage** - Optimized directory structure based on block hash

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
bsvlake-blockarchive = "0.1.0"
```

## Usage

```rust
use bsvlake_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive};
use bitcoinsv::bitcoin::BlockHash;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new archive with a root path
    let archive = SimpleFileBasedBlockArchive::new("/path/to/blockstore".to_string()).await?;
    
    // Check if a block exists
    let hash = BlockHash::from_str("...")?;
    let exists = archive.block_exists(&hash).await?;
    
    // Get a block
    if exists {
        let block = archive.get_block_full(&hash).await?;
        println!("Block has {} transactions", block.txdata.len());
    }
    
    // Store a block
    let new_block = /* ... */;
    archive.store_block_full(&new_block).await?;
    
    Ok(())
}
```

## Storage Structure

Blocks are stored in a hierarchical directory structure optimized for file system performance:

```
/root/
  /31/
    /c5/
      00000000000000000124a294b9e1e65224f0636ffd4dadac777bed5e709dc531.bin
```

The directory structure is based on the last characters of the block hash to distribute blocks evenly across directories.

## API

The main trait `BlockArchive` provides the following methods:

- `get_block()` - Get a reader for streaming a block
- `get_block_full()` - Load a complete block into memory
- `block_exists()` - Check if a block exists
- `store_block()` - Store a block from a reader
- `store_block_full()` - Store a complete block
- `block_size()` - Get the size of a stored block
- `block_tx_count()` - Get the transaction count in a block
- `block_header()` - Get just the block header
- `get_bytes_from_block()` - Get specific bytes from a block
- `block_list()` - Stream all block hashes in the archive

## Testing

Run the test suite:

```bash
cargo test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Related Projects

This crate is part of the [BSVLake](https://github.com/Danconnolly/bsvlake) project, a collection of tools and libraries for BitcoinSV blockchain data management.