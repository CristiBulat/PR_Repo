#!/usr/bin/env python3
"""
Integration Tests - Lab 4
Tests for the distributed key-value store with single-leader replication.

This script verifies:
1. Basic read/write operations
2. Replication to followers works correctly
3. Write quorum is enforced
4. Followers reject direct writes
5. Data consistency across nodes

Run with: python test_integration.py
Requires the system to be running via docker-compose up
"""

import requests
import time
import sys
from typing import Dict, Any, List


# =============================================================================
# CONFIGURATION
# =============================================================================

LEADER_URL = "http://localhost:5000"
FOLLOWER_URLS = [
    "http://localhost:5001",
    "http://localhost:5002",
    "http://localhost:5003",
    "http://localhost:5004",
    "http://localhost:5005",
]

# Test timeout
TIMEOUT = 5.0


# =============================================================================
# TEST UTILITIES
# =============================================================================

def wait_for_services(max_retries: int = 30, delay: float = 1.0) -> bool:
    """
    Wait for all services to be healthy.
    
    Args:
        max_retries: Maximum number of connection attempts
        delay: Delay between retries in seconds
    
    Returns:
        True if all services are healthy, False otherwise
    """
    print("Waiting for services to be ready...")
    
    all_urls = [LEADER_URL] + FOLLOWER_URLS
    
    for attempt in range(max_retries):
        all_healthy = True
        
        for url in all_urls:
            try:
                response = requests.get(f"{url}/health", timeout=2)
                if response.status_code != 200:
                    all_healthy = False
                    break
            except requests.exceptions.RequestException:
                all_healthy = False
                break
        
        if all_healthy:
            print("All services are healthy!")
            return True
        
        print(f"  Attempt {attempt + 1}/{max_retries}...")
        time.sleep(delay)
    
    print("Services not ready after maximum retries!")
    return False


def clear_all_stores() -> None:
    """Clear data from all nodes for a clean test environment."""
    print("Clearing all stores...")
    
    try:
        requests.post(f"{LEADER_URL}/clear", timeout=TIMEOUT)
        for url in FOLLOWER_URLS:
            requests.post(f"{url}/clear", timeout=TIMEOUT)
        time.sleep(0.5)  # Brief pause for consistency
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not clear all stores: {e}")


def print_test_header(test_name: str) -> None:
    """Print a formatted test header."""
    print(f"\n{'=' * 60}")
    print(f"TEST: {test_name}")
    print("=" * 60)


def print_result(passed: bool, message: str) -> None:
    """Print a test result."""
    status = "✅ PASSED" if passed else "❌ FAILED"
    print(f"{status}: {message}")


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

def test_leader_health() -> bool:
    """Test that the leader is healthy and responding."""
    print_test_header("Leader Health Check")
    
    try:
        response = requests.get(f"{LEADER_URL}/health", timeout=TIMEOUT)
        data = response.json()
        
        passed = (
            response.status_code == 200 and
            data.get('status') == 'healthy' and
            data.get('role') == 'leader'
        )
        
        print_result(passed, f"Leader status: {data.get('status')}, role: {data.get('role')}")
        print(f"  Write quorum: {data.get('write_quorum')}")
        print(f"  Followers: {data.get('followers')}")
        
        return passed
    
    except Exception as e:
        print_result(False, f"Error: {e}")
        return False


def test_followers_health() -> bool:
    """Test that all followers are healthy and responding."""
    print_test_header("Followers Health Check")
    
    all_passed = True
    
    for i, url in enumerate(FOLLOWER_URLS, 1):
        try:
            response = requests.get(f"{url}/health", timeout=TIMEOUT)
            data = response.json()
            
            passed = (
                response.status_code == 200 and
                data.get('status') == 'healthy' and
                data.get('role') == 'follower'
            )
            
            print_result(passed, f"Follower {i}: {data.get('status')}, role: {data.get('role')}")
            
            if not passed:
                all_passed = False
        
        except Exception as e:
            print_result(False, f"Follower {i} error: {e}")
            all_passed = False
    
    return all_passed


def test_basic_write_read() -> bool:
    """Test basic write to leader and read from leader."""
    print_test_header("Basic Write/Read (Leader)")
    
    clear_all_stores()
    
    try:
        # Write to leader
        write_response = requests.post(
            f"{LEADER_URL}/set",
            json={'key': 'test_key', 'value': 'test_value'},
            timeout=TIMEOUT
        )
        
        if write_response.status_code != 200:
            print_result(False, f"Write failed: {write_response.text}")
            return False
        
        print(f"  Write response: {write_response.json()}")
        
        # Read from leader
        read_response = requests.get(f"{LEADER_URL}/get/test_key", timeout=TIMEOUT)
        data = read_response.json()
        
        passed = (
            read_response.status_code == 200 and
            data.get('value') == 'test_value'
        )
        
        print_result(passed, f"Read value: {data.get('value')}")
        return passed
    
    except Exception as e:
        print_result(False, f"Error: {e}")
        return False


def test_replication() -> bool:
    """Test that writes are replicated to all followers."""
    print_test_header("Replication to Followers")
    
    clear_all_stores()
    
    try:
        # Write to leader
        test_key = 'replicated_key'
        test_value = 'replicated_value'
        
        write_response = requests.post(
            f"{LEADER_URL}/set",
            json={'key': test_key, 'value': test_value},
            timeout=TIMEOUT
        )
        
        if write_response.status_code != 200:
            print_result(False, f"Write failed: {write_response.text}")
            return False
        
        write_data = write_response.json()
        print(f"  Write to leader: success={write_data.get('success')}")
        print(f"  Replication: {write_data.get('replication', {}).get('confirmations', 0)} confirmations")
        
        # Brief pause for replication to complete
        time.sleep(0.5)
        
        # Read from all followers
        all_passed = True
        for i, url in enumerate(FOLLOWER_URLS, 1):
            read_response = requests.get(f"{url}/get/{test_key}", timeout=TIMEOUT)
            
            if read_response.status_code == 200:
                data = read_response.json()
                passed = data.get('value') == test_value
                print_result(passed, f"Follower {i}: value={data.get('value')}")
                if not passed:
                    all_passed = False
            else:
                print_result(False, f"Follower {i}: key not found")
                all_passed = False
        
        return all_passed
    
    except Exception as e:
        print_result(False, f"Error: {e}")
        return False


def test_follower_rejects_writes() -> bool:
    """Test that followers reject direct write requests."""
    print_test_header("Followers Reject Direct Writes")
    
    all_passed = True
    
    for i, url in enumerate(FOLLOWER_URLS, 1):
        try:
            write_response = requests.post(
                f"{url}/set",
                json={'key': 'illegal_key', 'value': 'illegal_value'},
                timeout=TIMEOUT
            )
            
            passed = write_response.status_code == 403
            print_result(passed, f"Follower {i}: status={write_response.status_code} (expected 403)")
            
            if not passed:
                all_passed = False
        
        except Exception as e:
            print_result(False, f"Follower {i} error: {e}")
            all_passed = False
    
    return all_passed


def test_multiple_writes() -> bool:
    """Test multiple sequential writes and verify consistency."""
    print_test_header("Multiple Writes Consistency")
    
    clear_all_stores()
    
    try:
        num_writes = 10
        
        # Perform multiple writes
        for i in range(num_writes):
            response = requests.post(
                f"{LEADER_URL}/set",
                json={'key': f'key_{i}', 'value': f'value_{i}'},
                timeout=TIMEOUT
            )
            
            if response.status_code != 200:
                print_result(False, f"Write {i} failed")
                return False
        
        print(f"  Wrote {num_writes} key-value pairs")
        
        # Brief pause for replication
        time.sleep(0.5)
        
        # Verify all data on leader
        leader_data = requests.get(f"{LEADER_URL}/all", timeout=TIMEOUT).json()
        leader_size = leader_data.get('size', 0)
        print(f"  Leader has {leader_size} entries")
        
        # Verify on followers
        all_passed = True
        for i, url in enumerate(FOLLOWER_URLS, 1):
            follower_data = requests.get(f"{url}/all", timeout=TIMEOUT).json()
            follower_size = follower_data.get('size', 0)
            
            passed = follower_size == num_writes
            print_result(passed, f"Follower {i}: {follower_size} entries (expected {num_writes})")
            
            if not passed:
                all_passed = False
        
        return all_passed
    
    except Exception as e:
        print_result(False, f"Error: {e}")
        return False


def test_data_consistency() -> bool:
    """Test that all nodes have exactly the same data."""
    print_test_header("Data Consistency Across All Nodes")
    
    clear_all_stores()
    
    try:
        # Write some data
        test_data = {
            'user:1': {'name': 'Alice', 'age': 30},
            'user:2': {'name': 'Bob', 'age': 25},
            'config': {'debug': True, 'version': '1.0'},
        }
        
        for key, value in test_data.items():
            response = requests.post(
                f"{LEADER_URL}/set",
                json={'key': key, 'value': value},
                timeout=TIMEOUT
            )
            if response.status_code != 200:
                print_result(False, f"Failed to write {key}")
                return False
        
        print(f"  Wrote {len(test_data)} entries to leader")
        
        # Brief pause for replication
        time.sleep(0.5)
        
        # Get data from leader
        leader_response = requests.get(f"{LEADER_URL}/all", timeout=TIMEOUT)
        leader_data = leader_response.json().get('data', {})
        
        # Compare with all followers
        all_passed = True
        for i, url in enumerate(FOLLOWER_URLS, 1):
            follower_response = requests.get(f"{url}/all", timeout=TIMEOUT)
            follower_data = follower_response.json().get('data', {})
            
            # Compare data
            if leader_data == follower_data:
                print_result(True, f"Follower {i}: data matches leader")
            else:
                print_result(False, f"Follower {i}: data mismatch!")
                print(f"    Leader has: {list(leader_data.keys())}")
                print(f"    Follower has: {list(follower_data.keys())}")
                all_passed = False
        
        return all_passed
    
    except Exception as e:
        print_result(False, f"Error: {e}")
        return False


def test_get_nonexistent_key() -> bool:
    """Test reading a key that doesn't exist returns 404."""
    print_test_header("Get Non-Existent Key (404)")
    
    try:
        response = requests.get(f"{LEADER_URL}/get/nonexistent_key_12345", timeout=TIMEOUT)
        
        passed = response.status_code == 404
        print_result(passed, f"Status code: {response.status_code} (expected 404)")
        
        return passed
    
    except Exception as e:
        print_result(False, f"Error: {e}")
        return False


# =============================================================================
# MAIN TEST RUNNER
# =============================================================================

def run_all_tests() -> Dict[str, bool]:
    """Run all integration tests and return results."""
    
    print("\n" + "=" * 60)
    print("INTEGRATION TESTS FOR LAB 4")
    print("Key-Value Store with Single-Leader Replication")
    print("=" * 60)
    
    # Wait for services
    if not wait_for_services():
        print("\n❌ FATAL: Services not available. Make sure to run 'docker-compose up' first!")
        sys.exit(1)
    
    # Run tests
    results = {}
    
    results['Leader Health'] = test_leader_health()
    results['Followers Health'] = test_followers_health()
    results['Basic Write/Read'] = test_basic_write_read()
    results['Replication'] = test_replication()
    results['Follower Rejects Writes'] = test_follower_rejects_writes()
    results['Multiple Writes'] = test_multiple_writes()
    results['Data Consistency'] = test_data_consistency()
    results['Get Non-Existent Key'] = test_get_nonexistent_key()
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅" if result else "❌"
        print(f"  {status} {test_name}")
    
    print("-" * 60)
    print(f"TOTAL: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
    
    return results


if __name__ == '__main__':
    results = run_all_tests()
    
    # Exit with appropriate code
    all_passed = all(results.values())
    sys.exit(0 if all_passed else 1)