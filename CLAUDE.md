# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Build and Test
- `cargo build` - Build the project
- `cargo test` - Run all tests
- `cargo test <test_name>` - Run a specific test
- `cargo clippy` - Run the Rust linter for code quality checks
- `cargo fmt` - Format code according to Rust style guidelines
- `cargo fmt --check` - Check if code is properly formatted without making changes

### Documentation
- `cargo doc --open` - Build and open the documentation in a browser

## Architecture Overview

BSVLake BlockArchive is a high-performance block storage library for Bitcoin SV, implemented as a Rust crate with async/await support via Tokio.

### Core Structure

The crate is organized around a trait-based architecture:

1. **`BlockArchive` trait** (`src/block_archive.rs`) - Defines the interface for any block storage implementation with methods for:
   - Reading blocks (streaming or full load)
   - Writing blocks
   - Querying block metadata (existence, size, transaction count, headers)
   - Listing all blocks via streaming

2. **`SimpleFileBasedBlockArchive`** (`src/sfb_archive.rs`) - The main implementation that:
   - Stores blocks in a hierarchical file system structure: `/root/<last-2-hash-chars>/<3rd-4th-last-chars>/<full-hash>.bin`
   - Optimizes for file system performance by distributing blocks across directories based on hash
   - Handles up to 2 million blocks (configurable via `MAX_BLOCKS` constant)
   - Provides async streaming of block lists without loading everything into memory

3. **Error handling** (`src/result.rs`) - Custom error types and Result alias for consistent error handling

### Key Design Decisions

- **Async-first**: All operations are async using Tokio, enabling high-performance concurrent operations
- **Streaming support**: Large operations like listing all blocks use streaming to avoid memory overhead
- **Hash-based distribution**: Files are distributed across directories based on block hash to avoid file system limitations with too many files in one directory
- **Minimal block knowledge**: The archive treats blocks as opaque data, only understanding enough structure for headers and transaction counts

### Dependencies

- `bitcoinsv` - Bitcoin SV protocol implementation
- `tokio` & `tokio-stream` - Async runtime and streaming
- `async-trait` - Async trait support
- `bytes` - Efficient byte buffer handling
- `hex` - Hex encoding/decoding for block hashes