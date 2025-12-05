#!/bin/bash
#
# Lab 4: Quorum Analysis Script
# 
# This script runs performance tests with different WRITE_QUORUM values (1-5)
# to generate data for the quorum vs latency analysis.
#
# Usage: ./run_quorum_tests.sh
#
# After running, use: python test_performance.py --plot
# to generate the comparison chart.

set -e

echo "========================================"
echo "Lab 4: Quorum Analysis"
echo "========================================"
echo ""
echo "This script will run performance tests with WRITE_QUORUM values 1-5"
echo "Each test makes ~10K concurrent writes"
echo ""

# Clear previous results
if [ -f "results.json" ]; then
    echo "Removing previous results.json..."
    rm results.json
fi

# Run tests for each quorum value
for QUORUM in 1 2 3 4 5; do
    echo ""
    echo "========================================"
    echo "Testing WRITE_QUORUM = $QUORUM"
    echo "========================================"
    
    # Stop existing containers
    docker-compose down 2>/dev/null || true
    
    # Update docker-compose.yml with new quorum value
    # Using sed to replace the WRITE_QUORUM value
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s/WRITE_QUORUM=[0-9]*/WRITE_QUORUM=$QUORUM/" docker-compose.yml
    else
        # Linux
        sed -i "s/WRITE_QUORUM=[0-9]*/WRITE_QUORUM=$QUORUM/" docker-compose.yml
    fi
    
    # Start containers
    echo "Starting containers with WRITE_QUORUM=$QUORUM..."
    docker-compose up -d --build
    
    # Wait for services to be ready
    echo "Waiting for services..."
    sleep 10
    
    # Run performance test (reduced threads to minimize contention)
    echo "Running performance test..."
    python3 test_performance.py --writes 2000 --keys 100 --threads 10
    
    echo ""
    echo "Completed test for WRITE_QUORUM=$QUORUM"
done

# Cleanup
echo ""
echo "========================================"
echo "All tests completed!"
echo "========================================"

# Reset quorum to default
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/WRITE_QUORUM=[0-9]*/WRITE_QUORUM=1/" docker-compose.yml
else
    sed -i "s/WRITE_QUORUM=[0-9]*/WRITE_QUORUM=1/" docker-compose.yml
fi

echo ""
echo "To generate the comparison plot, run:"
echo "  python test_performance.py --plot"
echo ""
echo "To see the analysis explanation, run:"
echo "  python test_performance.py --explain"