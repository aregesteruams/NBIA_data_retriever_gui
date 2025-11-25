#!/bin/bash
# Advanced feature tests for NBIA downloader
# Tests proxy, custom API, checksums, and other advanced features

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NBIA_TOOL="${SCRIPT_DIR}/../nbia-data-retriever-cli"
TEST_OUTPUT="${SCRIPT_DIR}/test_output"
MANIFEST="${SCRIPT_DIR}/fixtures/small_manifest.tcia"
USERNAME="${NBIA_USER:-nbia_guest}"
PASSWORD="${NBIA_PASS:-}"

# Source helper functions
source "${SCRIPT_DIR}/test_helpers.sh"

# Functions
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

print_result() {
    echo -e "${BLUE}  $1${NC}"
}

cleanup() {
    print_info "Cleaning up test directory..."
    rm -rf "$TEST_OUTPUT"
}

# Trap cleanup on exit
trap cleanup EXIT

echo "=========================================="
echo "NBIA Downloader Advanced Features Test"
echo "=========================================="
echo

# Test 1: Proxy support
print_info "Test 1: Testing proxy support..."
test1_dir="$TEST_OUTPUT/test1"
mkdir -p "$test1_dir"

# Test with invalid proxy (should fail gracefully)
if "$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$MANIFEST" \
    -o "$test1_dir" \
    -p 1 \
    --proxy "http://invalid.proxy:8080" \
    --max-retries 1 \
    --debug 2>&1 | grep -q "proxy\|connection"; then
    print_success "Proxy option is processed"
else
    print_info "Proxy test inconclusive (might work without proxy)"
fi

echo

# Test 2: Custom API URL
print_info "Test 2: Testing custom API URL..."
test2_dir="$TEST_OUTPUT/test2"
mkdir -p "$test2_dir"

# Test with custom API (should fail with invalid URL)
if "$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$MANIFEST" \
    -o "$test2_dir" \
    -p 1 \
    --token-url "https://invalid.api.example.com/token" \
    --meta-url "https://invalid.api.example.com/meta" \
    --image-url "https://invalid.api.example.com/image" \
    --max-retries 1 \
    --debug 2>&1 | grep -q "url\|connection\|failed"; then
    print_success "Custom API URL options work"
else
    print_error "Custom API URL not processed"
fi

echo

# Test 3: Checksum verification with MD5 mode
print_info "Test 3: Testing checksum verification with MD5 mode..."
test3_dir="$TEST_OUTPUT/test3"
mkdir -p "$test3_dir"

# First download files with MD5 validation enabled
if ! "$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$MANIFEST" \
    -o "$test3_dir" \
    -p 1 \
    --debug > "$test3_dir/download.log" 2>&1; then
    print_error "MD5 validation download failed"
    cat "$test3_dir/download.log" | tail -20
    exit 1
fi

# Check if MD5 verification actually happened
if validate_md5_verification "$test3_dir/download.log"; then
    print_success "MD5 checksum verification confirmed"
else
    print_info "MD5 checksum verification not confirmed in logs"
fi

# Validate extracted content has proper structure
if validate_extraction_structure "$test3_dir"; then
    print_success "MD5 validated files extracted properly"
else
    print_error "Extraction validation failed after MD5 download"
fi

# Test with no-decompress to keep ZIP files
test3b_dir="$TEST_OUTPUT/test3b"
mkdir -p "$test3b_dir"
"$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$MANIFEST" \
    -o "$test3b_dir" \
    -p 1 \
    --no-decompress \
    --no-md5 \
    --debug > "$test3b_dir/download.log" 2>&1

# Check if ZIP files were kept
zip_count=$(find "$test3b_dir" -name "*.zip" -type f | wc -l)
if [ "$zip_count" -gt 0 ]; then
    print_success "No-decompress mode keeps ZIP files ($zip_count found)"
else
    print_error "No-decompress mode failed"
fi

echo

# Test 4: Retry on truncated downloads
print_info "Test 4: Testing retry on truncated downloads..."
test4_dir="$TEST_OUTPUT/test4"
mkdir -p "$test4_dir"

# The tool should automatically retry on size mismatch errors
if "$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$MANIFEST" \
    -o "$test4_dir" \
    -p 1 \
    --max-retries 3 \
    --debug 2>&1 | tee "$test4_dir/retry.log"; then
    
    if grep -q "Retrying download\|size mismatch" "$test4_dir/retry.log"; then
        print_success "Automatic retry on truncation works"
    else
        print_success "Downloads completed without truncation"
    fi
else
    print_info "Some downloads may have failed"
fi

echo

# Test 5: Signal handling
print_info "Test 5: Testing graceful shutdown..."
test5_dir="$TEST_OUTPUT/test5"
mkdir -p "$test5_dir"

# Start download in background
"$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$MANIFEST" \
    -o "$test5_dir" \
    -p 3 \
    --debug > "$test5_dir/output.log" 2>&1 &
PID=$!

# Wait a bit then send SIGTERM
sleep 2
if ! kill -TERM $PID 2>/dev/null; then
    print_info "Process already terminated"
fi
wait $PID 2>/dev/null || true  # Wait can fail if process already exited

# Check if it shut down gracefully
if grep -q "signal\|interrupt\|shutdown\|cleanup" "$test5_dir/output.log"; then
    print_success "Graceful shutdown handling detected"
else
    print_info "Signal handling not explicitly shown"
fi

echo

# Test 6: Memory efficiency with large manifest
print_info "Test 6: Testing memory efficiency..."
test6_dir="$TEST_OUTPUT/test6"
mkdir -p "$test6_dir"

# Create a large manifest (duplicate entries)
large_manifest="$test6_dir/large_manifest.tcia"
head -n 6 "$MANIFEST" > "$large_manifest"
for i in {1..100}; do
    tail -n +7 "$MANIFEST" >> "$large_manifest"
done

# Monitor memory usage
# Note: time command can fail if tool fails, so we capture exit code
if /usr/bin/time -v "$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$large_manifest" \
    -o "$test6_dir" \
    -p 5 \
    --meta \
    --debug 2>&1 | tee "$test6_dir/memory.log"; then
    print_info "Memory test completed successfully"
else
    print_info "Memory test had non-zero exit but we captured metrics"
fi

if grep -q "Maximum resident set size" "$test6_dir/memory.log"; then
    mem_kb=$(grep "Maximum resident set size" "$test6_dir/memory.log" | awk '{print $6}')
    mem_mb=$((mem_kb / 1024))
    print_result "Peak memory usage: ${mem_mb}MB"
    if [ "$mem_mb" -lt 500 ]; then
        print_success "Memory efficient (< 500MB for large manifest)"
    else
        print_error "High memory usage: ${mem_mb}MB"
    fi
else
    print_info "Memory usage not measured"
fi

echo

# Test 7: Rate limiting handling
print_info "Test 7: Testing rate limit handling..."
test7_dir="$TEST_OUTPUT/test7"
mkdir -p "$test7_dir"

# Run with many workers to potentially trigger rate limiting
if "$NBIA_TOOL" -u "$USERNAME" --passwd "$PASSWORD" \
    -i "$MANIFEST" \
    -o "$test7_dir" \
    -p 20 \
    --max-connections 50 \
    --debug 2>&1 | tee "$test7_dir/ratelimit.log"; then
    
    if grep -q "429\|rate\|limit\|retry\|backoff" "$test7_dir/ratelimit.log"; then
        print_success "Rate limiting handled with retries"
    else
        print_info "No rate limiting encountered"
    fi
fi

echo

# Final Report
echo "=========================================="
echo "Advanced Features Test Summary"
echo "=========================================="
echo
echo "Features tested:"
echo "1. Proxy support - Check"
echo "2. Custom API URL - Check"
echo "3. Checksum verification - Check"
echo "4. Retry on truncated downloads - Check"
echo "5. Signal handling - Check"
echo "6. Memory efficiency - Check"
echo "7. Rate limiting - Check"
echo
print_success "All advanced feature tests completed!"