# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance Rust cache library called `flux-collaboration-rust-cache` that provides multiple caching strategies and JNI bridging capabilities for Java applications. The library specializes in large data storage with memory-mapped files, intelligent caching, and lease-based automatic cleanup mechanisms.

## Core Architecture

The codebase consists of several specialized storage layers:

1. **LocalCache** (`src/cache.rs`) - Main in-memory cache with lease-based auto-cleanup and disk offloading
2. **HighPerfMmapStorage** (`src/high_perf_mmap_storage.rs`) - High-performance memory-mapped file storage with hot data caching
3. **BigDataStorage** (`src/big_data_storage.rs`) - Specialized storage for large files (>1MB) with zero-copy optimization
4. **JNI Bridge Layer** - Multiple JNI implementations for Java integration:
   - `jni_bridge_datahub.rs` - Datahub-specific high-performance bridge
   - `jni_bridge_flatbuffers.rs` - FlatBuffers serialization bridge
   - `jni_bridge_high_perf.rs` - High-performance general bridge

### Key Features

- **Lease-based Auto-cleanup**: Automatic expiration with renewal capabilities
- **Disk Offloading**: Large values automatically spill to disk when exceeding thresholds
- **Memory-mapped Storage**: Zero-copy I/O for large datasets
- **Intelligent Caching**: LRU hot data caching with prefetch capabilities
- **Multiple Serialization**: Support for JSON, MessagePack, and FlatBuffers
- **Thread Safety**: Lock-free concurrent designs using DashMap and parking_lot

## Common Development Commands

### Build and Test
```bash
# Build the project
cargo build

# Build release version
cargo build --release

# Run tests
cargo test

# Run specific test
cargo test test_name

# Run performance benchmarks
cargo test --release performance_tests
```

### Code Quality
```bash
# Format code
cargo fmt

# Check code formatting
cargo fmt --all -- --check

# Run clippy lints
cargo clippy --all-targets --all-features -- -D warnings

# Run clippy with specific warnings allowed
cargo clippy --all-targets --all-features -- -W clippy::all
```

### JNI Library Building
```bash
# Build as cdylib for Java integration
cargo build --target-dir target/jni

# Create release build for production
cargo build --release --target-dir target/jni
```

## Key Configuration Files

- `Cargo.toml`: Main project configuration with JNI support (`cdylib` crate type)
- `rust-toolchain.toml`: Pins to stable Rust with required components
- Dependencies focus on: `moka` (caching), `memmap2` (memory mapping), `jni` (Java bridge), `dashmap` (concurrent HashMap), `parking_lot` (high-performance locks)

## Testing Strategy

The codebase includes comprehensive test coverage:

1. **Unit Tests**: Individual module tests in each source file
2. **Performance Tests**: Benchmarking in `src/performance_tests.rs`
3. **Integration Tests**: Cross-component functionality tests
4. **Stress Tests**: High-load scenarios for concurrent access

### Running Specific Test Categories
```bash
# Run only performance tests
cargo test performance_tests --release

# Run cache-specific tests
cargo test cache -- --nocapture

# Run JNI bridge tests
cargo test jni_bridge -- --nocapture
```

## Development Guidelines

### Adding New Storage Backends
1. Implement the core storage trait pattern
2. Add configuration struct following existing naming conventions
3. Include comprehensive error handling using `src/error_handling.rs`
4. Add performance metrics collection
5. Create corresponding JNI bridge if Java integration needed

### JNI Bridge Development
- Follow the naming convention: `Java_com_flux_collaboration_utils_cache_rust_jni_ClassName_methodName`
- Always handle `JNIEnv` conversion errors properly
- Use `unsafe` blocks judiciously and document safety assumptions
- Implement resource cleanup with explicit release methods

### Performance Considerations
- Use `parking_lot` instead of `std::sync` for better performance
- Leverage `DashMap` for concurrent HashMap operations
- Consider zero-copy techniques for large data operations
- Implement proper memory alignment for SIMD operations where applicable

## Memory Management

The library uses sophisticated memory management strategies:
- Hot data caching with LRU eviction
- Memory-mapped files for large dataset access
- Automatic disk offloading for oversized cache entries
- Lease-based cleanup to prevent memory leaks

## Error Handling

All modules use consistent error handling patterns defined in `src/error_handling.rs`. Errors are propagated using `Result<T, Box<dyn std::error::Error>>` for JNI compatibility and custom error types for internal operations.

## Serialization Support

The library supports multiple serialization formats:
- JSON: `serde_json` for general compatibility
- MessagePack: `rmp-serde` for performance (2-3x faster than JSON)
- FlatBuffers: `flatbuffers` for zero-copy deserialization (5-10x faster than JSON)

When working with serialization, prefer MessagePack or FlatBuffers for performance-critical paths.