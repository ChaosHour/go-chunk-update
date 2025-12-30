#!/bin/bash

# Comprehensive test script for go-chunk-update
# This script tests various combinations of options

echo "Testing go-chunk-update with various options..."

# Test 1: Help
echo "Test 1: Help"
./bin/go-chunk-update --help

# Test 2: Invalid command (no execute)
echo "Test 2: No execute option"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d test

# Test 3: Invalid query (no go_CHUNK)
echo "Test 3: Query without go_CHUNK"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d test -e "SELECT * FROM table"

# Test 4: Valid basic command (assuming test table exists)
echo "Test 4: Basic chunking"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10 -e "UPDATE detail_test SET column = value WHERE go_CHUNK(detail_test)" 2>/dev/null || echo "Expected to fail if table doesn't exist or no unique key"

# Test 5: With start-with and end-with
echo "Test 5: With start and end range"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10 --start-with=1 --end-with=100 -e "UPDATE detail_test SET column = value WHERE go_CHUNK(detail_test)" 2>/dev/null || echo "Expected to fail"

# Test 6: Force chunking column
echo "Test 6: Force chunking column"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10 --force-chunking-column=id:integer -e "UPDATE detail_test SET column = value WHERE go_CHUNK(detail_test)" 2>/dev/null || echo "Expected to fail"

# Test 7: With sleep
echo "Test 7: With sleep between chunks"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10 --sleep=100 -e "UPDATE detail_test SET column = value WHERE go_CHUNK(detail_test)" 2>/dev/null || echo "Expected to fail"

# Test 8: Verbose output
echo "Test 8: Verbose output"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10 -e "UPDATE detail_test SET column = value WHERE go_CHUNK(detail_test)" 2>/dev/null || echo "Expected to fail"

# Test 9: No log bin
echo "Test 9: No log bin"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10 --no-log-bin -e "UPDATE detail_test SET column = value WHERE go_CHUNK(detail_test)" 2>/dev/null || echo "Expected to fail"

# Test 10: Skip lock tables
echo "Test 10: Skip lock tables"
./bin/go-chunk-update --defaults-file=~/.my.cnf -d chaos -v -c 10 --skip-lock-tables -e "UPDATE detail_test SET column = value WHERE go_CHUNK(detail_test)" 2>/dev/null || echo "Expected to fail"

echo "Test script completed. Review the outputs above."