# Extensions Directory

This directory is reserved for future Mosaic features (v1.5+ and v2.0+).

## Purpose

The `extensions/` directory provides a structured namespace for forward-compatible features that will be added in future versions of Mosaic without breaking changes.

## Planned Extensions

### v1.5 - ACID Light (Q2 2025)
- Optimistic concurrency control
- Single-entry versioning
- Conflict detection and retry

### v2.0 - Full ACID (Q4 2025)
- Multi-entry transactions
- Two-phase commits
- Transaction coordinator
- Atomic batch operations

## Structure (Planned)

```
extensions/
├── README.md           # This file
├── transactions/       # v2.0+ transaction support
│   ├── active/        # Active transactions
│   ├── committed/     # Committed transactions log
│   └── rollback/      # Rollback data
└── versioning/        # v1.5+ version history
    └── history/       # Version history logs
```

## Forward Compatibility

The presence of this directory in v0.3.0 ensures that:
1. Future features can be added without breaking existing stores
2. Old clients can safely ignore unknown extensions
3. The file structure remains stable across versions

## Current Status (v0.3.0)

This directory is **reserved** but not yet used. All files within are placeholders for future functionality.
