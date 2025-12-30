# go-chunk-update

A Go port of the `oak-chunk-update` tool from the openarkkit suite, designed to safely execute large UPDATE, DELETE, and INSERT...SELECT operations by breaking them into manageable chunks.

## ⚠️ **Warning: Beta Software**

**This tool is currently in beta and may not be production-ready.** While it has been tested with various scenarios, it has not undergone extensive production testing. Use at your own risk, especially in production environments.

- **Backup your data** before using this tool
- **Test thoroughly** in a development environment first
- **Monitor performance** and resource usage during operations
- **Report any issues** to help improve the tool

## Overview

This tool helps prevent database performance issues and timeouts when executing large-scale data operations on MySQL tables. Instead of running a single massive query that could lock tables for extended periods or consume excessive resources, it processes the data in configurable chunks.

## Why a Go Port?

The original `oak-chunk-update` was written in Python as part of the openarkkit toolkit. This Go implementation provides:

- **Better Performance**: Compiled Go binary with lower memory overhead
- **Improved Maintainability**: Strong typing and modern Go idioms
- **Enhanced Security**: Parameterized queries prevent SQL injection
- **Cross-Platform**: Single binary that works on Linux, macOS, and Windows
- **Modern Dependencies**: Uses current MySQL drivers and libraries

## Features

- **Chunked Operations**: Process large datasets in configurable chunks
- **Multiple Query Types**: Supports UPDATE, DELETE, and INSERT...SELECT operations
- **Flexible Chunking**: Auto-detects unique keys or allows manual specification
- **Performance Controls**: Configurable sleep intervals and ratios
- **Safety Features**: Table locking, transaction handling, and error recovery
- **Verbose Output**: Detailed progress reporting matching the original tool's format
- **MySQL Integration**: Full support for MySQL configuration files and authentication

## Installation

### From Source

```bash
git clone https://github.com/yourusername/go-chunk-update.git
cd go-chunk-update
go build -o go-chunk-update ./cmd/chunk
```

### Using Go Install

```bash
go install github.com/ChaosHour/go-chunk-update/cmd/chunk@latest
```

### Releases

Pre-built binaries are available for each [release](https://github.com/ChaosHour/go-chunk-update/releases). Download the appropriate binary for your platform:

- **macOS (Intel/Apple Silicon)**: `go-chunk-update_Darwin_x86_64.tar.gz` / `go-chunk-update_Darwin_arm64.tar.gz`
- **Linux (Intel/ARM64)**: `go-chunk-update_Linux_x86_64.tar.gz` / `go-chunk-update_Linux_arm64.tar.gz`

To create a new release:

```bash
git tag v1.0.0  # Create a new version tag
git push origin v1.0.0  # Push tag to trigger automated release
```

## Usage

```bash
go-chunk-update --execute "UPDATE users SET status='active' WHERE GO_CHUNK(users)" \
                --database mydb \
                --chunk-size 1000 \
                --verbose
```

## Usage 2

```bash
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10000 --sleep=100 --skip-lock-tables -e "DELETE FROM detail WHERE GO_CHUNK(detail) AND id < 8496859"

-- Performing chunks range 8486548, 8496549, progress: 99%
-- + Rows: 10000 affected, 8490000 accumulating; seconds: 0.5 elapsed; 2277.1 executed
-- Performing chunks range 8496549, 8496868, progress: 99%
-- + Rows: 309 affected, 8490309 accumulating; seconds: 0.0 elapsed; 2277.2 executed
-- Performing chunks range 8496868, 8496868, progress: 100%
-- Performing chunks range complete. Affected rows: 8490309
-- Chunk update completed
```

### Key Options

- `--execute`: The query template with `GO_CHUNK(table_name)` placeholder
- `--chunk-size`: Number of rows to process per chunk (default: 1000)
- `--database`: Target database name
- `--verbose`: Enable detailed progress output
- `--sleep`: Milliseconds to sleep between chunks
- `--force-chunking-column`: Specify which column to use for chunking
- `--start-with`/`--end-with`: Define chunking range boundaries

### Example Queries

```bash
# Update operation
go-chunk-update -e "UPDATE large_table SET processed=1 WHERE GO_CHUNK(large_table)" -d mydb -v

# Delete operation
go-chunk-update -e "DELETE FROM old_records WHERE GO_CHUNK(old_records) AND created_at < '2020-01-01'" -d mydb

# Insert-select operation
go-chunk-update -e "INSERT INTO archive SELECT * FROM active_data WHERE GO_CHUNK(active_data)" -d mydb
```

## Real-World Examples

Based on production usage patterns, here are some practical examples of how `go-chunk-update` handles complex database operations:

```bash
# Large-scale user migration with JOIN and filtering
go-chunk-update --defaults-file=~/.my.cnf -d users_service_staging -v \
  --start-with=2323 --end-with=2860 --sleep=100 --skip-lock-tables \
  -e "INSERT INTO user (consumer_id, email_id, fname, lname) SELECT uuid, email, first_name, last_name FROM accounts WHERE GO_CHUNK(accounts)"

# GDPR data redaction on production logs
go-chunk-update --defaults-file=~/.my.cnf -d production -v --sleep=100 \
  --skip-lock-tables -e "UPDATE reservation_logs SET reservation_content=NULL WHERE GO_CHUNK(reservation_logs)"

# Bulk cleanup with subqueries
go-chunk-update --defaults-file=~/.my.cnf -d vis20_production -v --sleep=50 \
  --skip-lock-tables -e "DELETE FROM unit_price_summaries WHERE campaign_membership_coupon_id IN (SELECT id FROM temp) AND GO_CHUNK(unit_price_summaries)"

# Complex multi-table deletion with nested subqueries
go-chunk-update --defaults-file=~/.my.cnf -d production -v --sleep=50 \
  --skip-lock-tables -e "DELETE FROM redemption_sources WHERE redemption_id IN (SELECT id FROM redemptions WHERE membership_coupon_id IN (SELECT id FROM temp)) AND GO_CHUNK(redemption_sources)"

# Backfill operation with NULL handling
go-chunk-update --defaults-file=~/.my.cnf -d production -v --sleep=100 \
  -e "UPDATE rate_plan_daily_rate_product_set SET version=1493690023000 WHERE version IS NULL AND GO_CHUNK(rate_plan_daily_rate_product_set)"
```

## Testing

This project includes comprehensive testing infrastructure to ensure reliability and performance.

### Quick Test Run

```bash
# Run all tests including Docker MySQL environment
make test-docker

# Or run individual test phases
make test-start    # Start Docker MySQL
make test-unit     # Run Go unit tests
make test-integration  # Run integration tests
make test-perf     # Run performance tests
make test-stop     # Stop Docker MySQL
```

### Test Infrastructure

The testing setup includes:

- **Docker MySQL 8**: Primary/replica setup with realistic test data
- **Large Test Datasets**: 50K+ rows across multiple table scenarios
- **Real-World Scenarios**: Tests based on actual production usage patterns
- **Performance Benchmarks**: Chunk size optimization testing
- **Integration Tests**: Full CLI workflow validation

### Docker Environment

The `mysql8-docker/` directory contains a complete MySQL 8 testing environment:

```bash
cd mysql8-docker
make start  # Start MySQL containers
make stop   # Stop containers
make clean  # Remove containers and volumes
```

Test schemas include:

- `large_test_table`: 50K rows for performance testing
- `unit_price_summaries`: Complex subquery scenarios
- `archive_table`: INSERT...SELECT operations
- Multiple table relationships for JOIN testing

### CI/CD Pipeline

GitHub Actions automatically runs the full test suite on every push and PR:

- **Unit Tests**: Go test coverage with race detection
- **Integration Tests**: Full workflow testing with Docker MySQL
- **Performance Tests**: Benchmarking across different chunk sizes
- **Linting**: Code quality checks with golangci-lint
- **Cross-Platform Builds**: Automated releases for Linux, macOS, Windows

## Configuration

The tool supports MySQL configuration files:

```bash
go-chunk-update --defaults-file ~/.my.cnf --execute "..." --database mydb
```

## Safety Features

- **Table Locking**: Prevents concurrent modifications during chunking
- **Transaction Safety**: Each chunk is processed atomically
- **Error Recovery**: Continues processing even if individual chunks fail
- **Progress Tracking**: Shows completion percentage and estimated time remaining

## Differences from Original

This Go port maintains full compatibility with the original Python version while adding improvements:

- **SQL Injection Protection**: All queries use parameterized statements
- **Better Error Messages**: More detailed error reporting with context
- **Composite Key Support**: Enhanced handling of multi-column primary keys
- **NULL Value Handling**: Proper filtering of NULL values in range calculations
- **Modern MySQL Driver**: Uses the latest `go-sql-driver/mysql` with connection pooling

## Credits

This tool is a port of the original `oak-chunk-update` from the [openarkkit](http://code.openark.org/forge/openark-kit) project by **Shlomi Noach**.

- Original Author: Shlomi Noach
- Original License: BSD 3-Clause
- Go Port Author: Kurt Larsen
- Go Port Date: December 2025

Shlomi Noach is an inspiration and an amazing engineer whose work on openarkkit has been invaluable to the MySQL community.

## License

This project is released under the BSD 3-Clause License, same as the original openarkkit project. See [LICENSE](LICENSE) for details.
