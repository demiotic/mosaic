# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in Mosaic, please report it by emailing the maintainers directly. **Do not open a public issue.**

We will acknowledge your email within 48 hours and provide a detailed response within 5 business days indicating the next steps in handling your report.

## Threat Model

### In Scope

Mosaic is designed for **trusted environments** where:
- Storage backends (S3, local filesystem) are trusted
- Network connections are secured (HTTPS/TLS)
- Writers are authenticated and authorized at the infrastructure level
- Readers have appropriate access controls (IAM, file permissions)

### Assumptions

1. **Trusted Storage**: Storage backends (S3, local filesystem) are not adversarial
2. **Authenticated Writers**: Writer identity is managed by external IAM/authentication systems
3. **Secure Network**: S3 API calls use HTTPS; no man-in-the-middle attacks on storage operations
4. **File System Permissions**: Local storage is protected by appropriate OS-level permissions

### Security Properties

#### Data Integrity

1. **Content-Addressed Storage**: SHA256 hashing ensures data integrity
   - Collision detection prevents SHA256 collisions
   - Sample check (first 64KB) + size check + full hash verification
   - Automatic deduplication with strong verification

2. **Checksums**: All snapshots and indexes include checksums
   - SHA256 checksums for all blobs
   - Manifest includes checksum for every snapshot and index
   - Verification on read operations

3. **Immutability**: Append-only architecture
   - No in-place modifications of existing data
   - Snapshots are write-once, read-many
   - Compaction creates new snapshots rather than modifying existing ones

#### Concurrency Safety

1. **Optimistic Locking**: ETag-based conditional writes prevent race conditions
   - Manifest updates use optimistic locking
   - Automatic retry with exponential backoff
   - Max 5 attempts before failure

2. **WAL for Crash Safety**: Write-Ahead Log prevents data loss
   - Pending writes registered before blob storage
   - Heartbeat mechanism (30s intervals)
   - Stale writer cleanup (2x TTL = 120s)
   - Automatic recovery on restart

3. **Lease-Based Compaction**: Only one writer compacts at a time
   - Lease acquisition with conditional writes
   - Lease TTL: 300 seconds
   - Two-phase commit with temp/ staging

#### Availability & Resilience

1. **Circuit Breaker**: Protects against S3 degradation
   - Opens at 50% error rate (configurable)
   - Timeout: 60 seconds before testing recovery
   - Prevents cascading failures

2. **Rate Limiting**: Token bucket prevents S3 throttling
   - Default: 3000 tokens/second (S3 PUT limit)
   - Refill rate: 1000 tokens/second
   - Automatic backoff on exceeded limits

3. **Retry Logic**: Exponential backoff with jitter
   - Max retries: 5 attempts
   - Initial delay: 100ms
   - Max delay: 30 seconds
   - Jitter: ±20%

### Out of Scope

The following are **not** defended against:

1. **Malicious Storage Backend**: If S3 or the local filesystem is compromised, an attacker can:
   - Read all stored data (encrypted storage backends should be used for sensitive data)
   - Modify or delete blobs, snapshots, and indexes
   - Prevent new writes

2. **Authentication & Authorization**: Mosaic does not implement:
   - User authentication
   - Access control lists (ACLs)
   - Writer identity verification
   - Reader permission checks

   These must be handled by:
   - AWS IAM for S3 backends
   - File system permissions for local backends
   - Application-level authentication

3. **Encryption**: Mosaic does not provide:
   - At-rest encryption (use S3 server-side encryption or filesystem encryption)
   - In-transit encryption (use HTTPS for S3, secure connections for network storage)
   - Key management

4. **Denial of Service (DoS)**: While rate limiting protects against accidental overload:
   - No protection against intentional DoS attacks on the storage backend
   - No protection against malicious writers flooding the system with entries
   - No protection against resource exhaustion attacks

5. **Side-Channel Attacks**: No protection against:
   - Timing attacks on hash comparisons
   - Power analysis
   - Cache timing attacks

6. **Physical Security**: No protection against:
   - Physical access to storage media
   - Hardware tampering
   - Memory dumps

### Secure Deployment Recommendations

1. **Storage Backend**:
   - Use S3 with server-side encryption (SSE-S3, SSE-KMS)
   - Enable S3 versioning for additional protection
   - Configure S3 bucket policies to restrict access
   - Enable S3 access logging for audit trails

2. **Network**:
   - Always use HTTPS for S3 connections
   - Use VPC endpoints for S3 in AWS
   - Restrict network access to storage backends

3. **Authentication**:
   - Use IAM roles for S3 access (avoid long-lived credentials)
   - Rotate credentials regularly
   - Use least-privilege IAM policies
   - Consider using temporary credentials (STS)

4. **Monitoring**:
   - Monitor S3 access logs for suspicious activity
   - Set up CloudWatch alarms for unusual access patterns
   - Track WAL stale writer cleanup (potential sign of crashes or attacks)
   - Monitor circuit breaker open states

5. **Data Classification**:
   - Do not store sensitive data (PII, secrets) without encryption
   - Use application-level encryption for sensitive content before storing
   - Consider using AWS KMS for key management

6. **Multi-Writer Security**:
   - Ensure each writer has a unique, verifiable ID
   - Monitor writer behavior (entry rate, content types)
   - Set up alerts for stale writers
   - Implement application-level writer authentication

## Known Limitations

1. **Dependency Vulnerabilities**:
   - Transitive dependency on `paste` crate (unmaintained, no known vulnerabilities)
   - Transitive dependency on `proc-macro-error` crate (unmaintained, no known vulnerabilities)
   - Run `cargo audit` regularly to check for new advisories

2. **Presigned URL Security**:
   - Presigned URLs have a TTL (default: 3600 seconds)
   - URLs can be shared; consider if this fits your security model
   - URLs are not encrypted; assume network visibility

3. **WAL Cleanup**:
   - Stale writers cleaned up after 2x TTL (120 seconds)
   - Potential race condition if writer crashes during cleanup
   - Cleanup is conservative to avoid false positives

## Security Updates

Security updates will be released as patch versions (e.g., 1.0.1, 1.0.2) and will be announced:
- In the CHANGELOG.md
- In GitHub releases
- Via security advisories (if applicable)

## Security Best Practices

1. **Keep Dependencies Updated**: Run `cargo update` regularly
2. **Run Security Audits**: Run `cargo audit` before releases
3. **Monitor Storage**: Set up alerts for unusual access patterns
4. **Test Disaster Recovery**: Regularly test WAL recovery and stale writer cleanup
5. **Backup Manifest**: The manifest is critical; back it up regularly
6. **Review Access Logs**: Audit S3/storage access logs periodically
7. **Rotate Credentials**: Rotate storage credentials according to your security policy

## Compliance

Mosaic does not currently meet any specific compliance standards (HIPAA, SOC 2, etc.). If you require compliance:
- Implement encryption at rest using storage backend features
- Implement comprehensive access logging
- Implement audit trails at the application level
- Implement data retention policies
- Consult with a compliance expert

## Contact

For security concerns or questions:
- Email: juanjo@delasheras.dev
- Security advisories: https://github.com/demiotic/mosaic/security/advisories
