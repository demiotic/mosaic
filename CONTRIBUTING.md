# Contributing to Mosaic

Thank you for your interest in contributing to Mosaic! This document provides guidelines and information for contributors.

## Development Setup

### Prerequisites

- Rust 1.75+ (latest stable recommended)
- Docker (optional, for MinIO testing)
- Git

### Clone and Build

```bash
git clone https://github.com/your-org/mosaic.git
cd mosaic/crates/mosaic-core

# Build with default features (local + memory backends)
cargo build

# Build with all features
cargo build --all-features
```

## Running Tests

### Unit Tests (No External Dependencies)

The default test suite uses in-memory and local filesystem backends - **no cloud services required**:

```bash
# Run all unit tests
cargo test

# Run with verbose output
cargo test -- --nocapture

# Run specific backend tests
cargo test storage::backends::memory
cargo test storage::backends::local
```

These tests should **always pass** and are required for all contributions.

### S3 Backend Tests with MinIO

For testing S3 functionality, we use MinIO (an S3-compatible server) instead of requiring AWS credentials.

#### 1. Start MinIO

```bash
docker run -d -p 9000:9000 -p 9001:9001 \
  --name mosaic-minio \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

#### 2. Create Test Bucket

```bash
# Set up MinIO client alias
docker exec mosaic-minio mc alias set local http://localhost:9000 minioadmin minioadmin

# Create test bucket
docker exec mosaic-minio mc mb local/test-bucket
```

#### 3. Run S3 Backend Tests

```bash
# Run S3 backend tests (marked as #[ignore] by default)
cargo test --features backend-s3 -- --ignored

# Run S3 integration example
cargo run --example minio_integration --features backend-s3
```

#### 4. Verify Results

```bash
# List files in MinIO
docker exec mosaic-minio mc ls -r local/test-bucket/

# View MinIO console
open http://localhost:9001  # Login: minioadmin / minioadmin
```

#### 5. Clean Up

```bash
# Stop and remove MinIO container
docker stop mosaic-minio
docker rm mosaic-minio
```

### Testing Checklist for Contributors

Before submitting a PR, ensure:

- [ ] `cargo test` passes (default backends)
- [ ] `cargo test --all-features` passes (if you modified backend code)
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo fmt --check` passes
- [ ] If you modified S3 backend: MinIO integration tests pass
- [ ] Examples still work: `cargo run --example local_storage`
- [ ] Documentation is updated (if applicable)

### Continuous Integration

Our CI runs the following checks on every PR:

```yaml
# Default tests (no external dependencies)
- cargo test

# Linting
- cargo clippy -- -D warnings
- cargo fmt --check

# Build all feature combinations
- cargo build --no-default-features
- cargo build --features backend-local
- cargo build --features backend-memory
- cargo build --features backend-s3
- cargo build --all-features

# Examples
- cargo build --example local_storage
- cargo build --example minio_integration --features backend-s3
```

**Note:** S3/MinIO integration tests (`--ignored` tests) are run in CI with a MinIO service container.

## Code Style

### Rust Style

We follow the standard Rust style guide:

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy -- -D warnings
```

### Naming Conventions

- **Backends**: `{Name}Backend` (e.g., `S3Backend`, `LocalBackend`)
- **Tests**: `test_{functionality}` (e.g., `test_put_and_get`)
- **Examples**: `{use_case}_example` (e.g., `minio_integration`)

### Documentation

- All public APIs must have doc comments
- Examples in doc comments should compile (use `no_run` if they require external services)
- Update README.md for user-facing changes
- Update TESTING.md for new testing procedures

## Adding a New Storage Backend

To add a new storage backend (e.g., Azure, GCS):

### 1. Add Feature Flag

In `Cargo.toml`:

```toml
[features]
backend-azure = ["azure-storage-blobs"]  # Add dependencies

[dependencies]
azure-storage-blobs = { version = "0.x", optional = true }
```

### 2. Implement the Backend

Create `src/storage/backends/azure.rs`:

```rust
use async_trait::async_trait;
use crate::error::Result;
use crate::storage::backend::{ObjectStore, ObjectStoreConfig};

#[derive(Debug, Clone)]
pub struct AzureBackend {
    config: ObjectStoreConfig,
    // ... Azure client
}

impl AzureBackend {
    pub fn new(config: ObjectStoreConfig) -> Result<Self> {
        // Initialize Azure client
        todo!()
    }
}

#[async_trait]
impl ObjectStore for AzureBackend {
    async fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        todo!()
    }

    // ... implement all ObjectStore methods
}
```

### 3. Register in Builder

In `src/storage/backend.rs`:

```rust
pub enum BackendType {
    S3,
    Azure,  // Add here
    Gcs,
    Local,
    Memory,
}

impl ObjectStoreBuilder {
    pub async fn build(self) -> Result<Box<dyn ObjectStore>> {
        match self.backend_type {
            // ... existing backends

            #[cfg(feature = "backend-azure")]
            BackendType::Azure => {
                use super::backends::azure::AzureBackend;
                Ok(Box::new(AzureBackend::new(self.config)?))
            }
            #[cfg(not(feature = "backend-azure"))]
            BackendType::Azure => {
                Err(MosaicError::InvalidEntry(
                    "Azure backend not enabled. Enable with --features backend-azure".to_string()
                ))
            }
        }
    }
}
```

### 4. Add Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let backend = AzureBackend::new(test_config()).unwrap();
        backend.put("key", b"value".to_vec()).await.unwrap();
        let data = backend.get("key").await.unwrap();
        assert_eq!(data, b"value");
    }

    // Add comprehensive tests for all ObjectStore methods
}
```

### 5. Update Documentation

- Add backend to README.md
- Add testing instructions to TESTING.md and CONTRIBUTING.md
- Add example to `examples/azure_integration.rs`

## Pull Request Process

1. **Fork and Branch**
   ```bash
   git checkout -b feature/my-new-feature
   ```

2. **Make Changes**
   - Write code following our style guide
   - Add tests for new functionality
   - Update documentation

3. **Test Locally**
   ```bash
   cargo test
   cargo clippy -- -D warnings
   cargo fmt
   ```

4. **Commit**
   ```bash
   git commit -m "feat: add new storage backend for Azure"
   ```

   Use conventional commit format:
   - `feat:` - New feature
   - `fix:` - Bug fix
   - `docs:` - Documentation changes
   - `test:` - Test changes
   - `refactor:` - Code refactoring
   - `perf:` - Performance improvements

5. **Push and Create PR**
   ```bash
   git push origin feature/my-new-feature
   ```

   Create a pull request with:
   - Clear description of changes
   - Link to related issues
   - Screenshots/examples if applicable

6. **Code Review**
   - Address review feedback
   - Ensure CI passes
   - Squash commits if requested

## Common Development Tasks

### Adding a New Example

```bash
# Create example file
touch examples/my_example.rs

# Test it
cargo run --example my_example

# Add to CI (if applicable)
```

### Debugging Tests

```bash
# Run with logging
RUST_LOG=debug cargo test -- --nocapture

# Run specific test
cargo test test_name -- --nocapture

# Run with backtrace
RUST_BACKTRACE=1 cargo test
```

### Benchmarking

```bash
# Run benchmarks (if added)
cargo bench

# Profile with flamegraph
cargo install flamegraph
cargo flamegraph --example local_storage
```

## Getting Help

- **Issues**: Open a GitHub issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check README.md and TESTING.md first

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Follow Rust community guidelines

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT OR Apache-2.0).

---

Thank you for contributing to Mosaic! 🎉
