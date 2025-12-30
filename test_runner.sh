#!/bin/bash

# Comprehensive testing script for go-chunk-update using Docker MySQL 8
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/mysql8-docker"
TEST_DB="chunk_test"

echo "🚀 Starting comprehensive go-chunk-update testing..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
}

# Start MySQL containers
start_mysql() {
    log_info "Starting MySQL 8 containers..."

    if [ ! -d "$DOCKER_DIR" ]; then
        log_error "Docker directory not found: $DOCKER_DIR"
        log_error "Current directory: $(pwd)"
        log_error "Project root: $PROJECT_ROOT"
        ls -la "$PROJECT_ROOT" || true
        exit 1
    fi

    cd "$DOCKER_DIR"

    # Create .env file if it doesn't exist
    if [ ! -f .env ]; then
        echo "MYSQL_ROOT_PASSWORD=s3cr3t" > .env
        log_warning "Created .env file with default password"
    fi

    make start
    log_success "MySQL containers started"

    # Wait for containers to be healthy
    log_info "Waiting for MySQL to be ready..."
    sleep 30

    # Check if containers are healthy
    if ! docker ps | grep -q "mysql-primary\|mysql-replica"; then
        log_error "MySQL containers failed to start"
        exit 1
    fi

    log_success "MySQL containers are ready"
}

# Initialize test database
init_test_db() {
    log_info "Initializing test database..."

    # Copy test schema to container and run it
    docker cp "$DOCKER_DIR/primary/chunk_test_schema.sql" mysql-primary:/tmp/

    # Run the schema
    docker exec mysql-primary mysql -u root -ps3cr3t < "$DOCKER_DIR/primary/chunk_test_schema.sql"

    log_success "Test database initialized"
}

# Run Go unit tests
run_unit_tests() {
    log_info "Running Go unit tests..."
    cd "$PROJECT_ROOT"
    go test ./... -v
    log_success "Unit tests completed"
}

# Run integration tests
run_integration_tests() {
    log_info "Running integration tests..."

    cd "$PROJECT_ROOT"

    # Build the binary
    make build

    # Test 1: Basic functionality
    log_info "Test 1: Basic help command"
    ./bin/go-chunk-update --help | grep -q "go-chunk-update" || {
        log_error "Help command failed"
        exit 1
    }

    # Test 2: Invalid query (should fail)
    log_info "Test 2: Invalid query validation"
    ./bin/go-chunk-update -e "SELECT * FROM test" -d "$TEST_DB" 2>&1 | grep -q "Query must contain GO_CHUNK" || {
        log_error "Query validation failed"
        exit 1
    }

    # Test 3: Large table operations (using Docker MySQL)
    log_info "Test 3: Large table chunking operations"

    # Test UPDATE operation
    ./bin/go-chunk-update \
        --defaults-file="$DOCKER_DIR/.env" \
        -d "$TEST_DB" \
        -v \
        -c 1000 \
        --force-chunking-column=MCID:integer \
        --sleep=10 \
        -e "UPDATE large_test_table SET data = CONCAT(data, ' UPDATED') WHERE GO_CHUNK(large_test_table)" \
        2>&1 | tee /tmp/test_output.log

    if grep -q "Chunk update completed" /tmp/test_output.log; then
        log_success "UPDATE operation successful"
    else
        log_error "UPDATE operation failed"
        cat /tmp/test_output.log
        exit 1
    fi

    # Test INSERT operation
    ./bin/go-chunk-update \
        --defaults-file="$DOCKER_DIR/.env" \
        -d "$TEST_DB" \
        -v \
        -c 500 \
        --force-chunking-column=id:integer \
        --sleep=5 \
        -e "INSERT INTO archive_table SELECT * FROM large_test_table WHERE GO_CHUNK(large_test_table)" \
        2>&1 | tee /tmp/test_output2.log

    if grep -q "Chunk update completed" /tmp/test_output2.log; then
        log_success "INSERT operation successful"
    else
        log_error "INSERT operation failed"
        cat /tmp/test_output2.log
        exit 1
    fi

    # Test DELETE operation with subquery
    ./bin/go-chunk-update \
        --defaults-file="$DOCKER_DIR/.env" \
        -d "$TEST_DB" \
        -v \
        -c 100 \
        --force-chunking-column=id:integer \
        --sleep=5 \
        -e "DELETE FROM unit_price_summaries WHERE campaign_membership_coupon_id IN (SELECT id FROM temp_cleanup) AND GO_CHUNK(unit_price_summaries)" \
        2>&1 | tee /tmp/test_output3.log

    if grep -q "Chunk update completed" /tmp/test_output3.log; then
        log_success "DELETE operation successful"
    else
        log_error "DELETE operation failed"
        cat /tmp/test_output3.log
        exit 1
    fi

    log_success "Integration tests completed"
}

# Performance testing
run_performance_tests() {
    log_info "Running performance tests..."

    cd "$PROJECT_ROOT"

    # Test with different chunk sizes
    for chunk_size in 100 1000 5000; do
        log_info "Testing with chunk size: $chunk_size"

        start_time=$(date +%s)

        ./bin/go-chunk-update \
            --defaults-file="$DOCKER_DIR/.env" \
            -d "$TEST_DB" \
            -c "$chunk_size" \
            --force-chunking-column=MCID:integer \
            --sleep=1 \
            -e "UPDATE large_test_table SET updated_at = NOW() WHERE GO_CHUNK(large_test_table)" \
            >/dev/null 2>&1

        end_time=$(date +%s)
        duration=$((end_time - start_time))

        log_success "Chunk size $chunk_size completed in ${duration}s"
    done
}

# Clean up
cleanup() {
    log_info "Cleaning up..."

    cd "$DOCKER_DIR"
    make stop
    make clean

    # Remove test outputs
    rm -f /tmp/test_output*.log

    log_success "Cleanup completed"
}

# Main execution
main() {
    case "${1:-all}" in
        "start")
            check_docker
            start_mysql
            init_test_db
            ;;
        "test")
            run_unit_tests
            run_integration_tests
            ;;
        "perf")
            run_performance_tests
            ;;
        "stop")
            cleanup
            ;;
        "all")
            check_docker
            start_mysql
            init_test_db
            run_unit_tests
            run_integration_tests
            run_performance_tests
            cleanup
            log_success "All tests completed successfully! 🎉"
            ;;
        *)
            echo "Usage: $0 {start|test|perf|stop|all}"
            echo "  start - Start MySQL containers and init DB"
            echo "  test  - Run unit and integration tests"
            echo "  perf  - Run performance tests"
            echo "  stop  - Stop containers and cleanup"
            echo "  all   - Run complete test suite"
            exit 1
            ;;
    esac
}

main "$@"