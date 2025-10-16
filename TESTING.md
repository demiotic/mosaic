# Testing Guide

This guide explains how to run tests for Mosaic Core with different storage backends.

## Unit Tests (Default)

The default tests use in-memory and local filesystem backends:

```bash
cargo test --package mosaic-core
```

This runs all unit tests without requiring external dependencies.

## S3 Backend Tests

### MinIO (S3-Compatible Local Testing)

MinIO provides an S3-compatible server for local testing. This is the recommended way to test S3 functionality without using AWS.

#### 1. Start MinIO Server

Using Docker:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  --name mosaic-minio \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001 (admin UI)
- Default credentials: `minioadmin` / `minioadmin`

#### 2. Create Test Bucket

Open the MinIO console at http://localhost:9001 or use the MinIO client:

```bash
# Using mc (MinIO Client)
mc alias set myminio http://localhost:9000 minioadmin minioadmin
mc mb myminio/test-bucket
```

#### 3. Run S3 Backend Tests

```bash
# Run ignored tests that require MinIO
cargo test --package mosaic-core --features backend-s3 -- --ignored

# Or run the MinIO integration example
cargo run --example minio_integration --features backend-s3
```

### AWS S3 (Production)

To test with real AWS S3:

1. Configure AWS credentials:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

2. Create test bucket (if needed):

```bash
aws s3 mb s3://mosaic-test-bucket
```

3. Run tests or examples with the S3 backend feature enabled:

```bash
cargo test --package mosaic-core --features backend-s3
```

## Local Backend Tests

The local backend tests run automatically with the default test command. They use temporary directories that are cleaned up after tests complete.

```bash
cargo test --package mosaic-core storage::backends::local
```

## Memory Backend Tests

Memory backend tests are the fastest and require no external dependencies:

```bash
cargo test --package mosaic-core storage::backends::memory
```

## Running All Tests with All Backends

To run tests with all available backends:

```bash
# All backends except S3 (no external dependencies)
cargo test --package mosaic-core --features backend-local,backend-memory

# All backends including S3 (requires MinIO or AWS)
cargo test --package mosaic-core --all-features
```

## Backend Feature Flags

- `backend-memory` - In-memory storage (default, for testing)
- `backend-local` - Local filesystem storage (default)
- `backend-s3` - AWS S3 / MinIO / S3-compatible storage
- `backend-azure` - Azure Blob Storage (planned)
- `backend-gcs` - Google Cloud Storage (planned)

Default features: `backend-memory`, `backend-local`

## Continuous Integration

For CI environments, use the memory and local backends which don't require external services:

```yaml
- name: Run tests
  run: cargo test --package mosaic-core
```

To test S3 functionality in CI, you can start MinIO as a service:

```yaml
services:
  minio:
    image: minio/minio
    ports:
      - 9000:9000
    env:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data

steps:
  - name: Run S3 tests
    run: cargo test --package mosaic-core --features backend-s3 -- --ignored
```

## Troubleshooting

### MinIO Connection Refused

If you get connection errors:

1. Check MinIO is running: `docker ps`
2. Verify port 9000 is accessible: `curl http://localhost:9000`
3. Check credentials match the example

### AWS Credentials Not Found

If AWS tests fail with credentials errors:

1. Ensure AWS credentials are configured: `aws configure`
2. Or set environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
3. Verify credentials: `aws sts get-caller-identity`

### Test Bucket Already Exists

If you get "bucket already exists" errors:

- Use a unique bucket name
- Or delete the existing bucket: `aws s3 rb s3://bucket-name --force`
- For MinIO: Delete bucket via console at http://localhost:9001
