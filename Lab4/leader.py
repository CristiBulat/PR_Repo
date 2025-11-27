"""
Leader Node - Lab 4
Implements the leader node for the distributed key-value store.

The leader:
- Accepts all write requests from clients
- Replicates writes to followers with simulated network delay
- Uses semi-synchronous replication with configurable write quorum
- Handles concurrent requests using a thread pool
"""

import os
import sys
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from flask import Flask, request, jsonify
from kv_store import KeyValueStore
import requests
from datetime import datetime

# =============================================================================
# CONFIGURATION - Read from environment variables
# =============================================================================

# List of follower URLs (comma-separated)
FOLLOWERS = os.environ.get('FOLLOWERS', '').split(',')
FOLLOWERS = [f.strip() for f in FOLLOWERS if f.strip()]

# Write quorum: number of follower confirmations required for a successful write
# This implements semi-synchronous replication
WRITE_QUORUM = int(os.environ.get('WRITE_QUORUM', 1))

# Network delay simulation (in seconds)
MIN_DELAY = float(os.environ.get('MIN_DELAY', 0.0001))  # ~0.1ms
MAX_DELAY = float(os.environ.get('MAX_DELAY', 0.01))    # ~10ms

# Server configuration
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', 5000))

# Thread pool size for concurrent replication
REPLICATION_WORKERS = int(os.environ.get('REPLICATION_WORKERS', 10))

# Request timeout for follower replication (seconds)
REPLICATION_TIMEOUT = float(os.environ.get('REPLICATION_TIMEOUT', 5.0))

# =============================================================================
# INITIALIZATION
# =============================================================================

app = Flask(__name__)
store = KeyValueStore()

# Thread pool for concurrent replication to followers
replication_executor = ThreadPoolExecutor(max_workers=REPLICATION_WORKERS)

# Statistics tracking
stats = {
    'writes_total': 0,
    'writes_successful': 0,
    'writes_failed': 0,
    'replication_successes': 0,
    'replication_failures': 0,
    'start_time': datetime.utcnow().isoformat()
}
stats_lock = threading.Lock()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_simulated_delay() -> float:
    """
    Generate a random delay to simulate network latency.
    
    Returns:
        A random delay between MIN_DELAY and MAX_DELAY seconds
    
    This simulates real-world network conditions where different
    followers may have different latencies.
    """
    return random.uniform(MIN_DELAY, MAX_DELAY)


def replicate_to_follower(follower_url: str, key: str, value, version: int) -> dict:
    """
    Replicate a write operation to a single follower.
    
    This function:
    1. Simulates network delay before sending the request
    2. Sends the write request to the follower
    3. Returns the result (success/failure)
    
    Args:
        follower_url: The base URL of the follower node
        key: The key being written
        value: The value being written
        version: The version number of this write
    
    Returns:
        A dictionary with 'success' (bool) and 'follower' (str) fields
    
    Preconditions:
        - follower_url is a valid URL string
        - key is a non-empty string
        - value is JSON-serializable
        - version is a positive integer
    
    Postconditions:
        - Returns success status for this specific follower
        - Network delay is applied before the request
    """
    # Simulate network delay BEFORE sending the request (as per lab requirements)
    delay = get_simulated_delay()
    time.sleep(delay)
    
    try:
        response = requests.post(
            f"{follower_url}/replicate",
            json={
                'key': key,
                'value': value,
                'version': version
            },
            timeout=REPLICATION_TIMEOUT
        )
        
        if response.status_code == 200:
            return {'success': True, 'follower': follower_url, 'delay': delay}
        else:
            return {'success': False, 'follower': follower_url, 'error': response.text}
    
    except requests.exceptions.RequestException as e:
        return {'success': False, 'follower': follower_url, 'error': str(e)}


def replicate_to_all_followers(key: str, value, version: int) -> dict:
    """
    Replicate a write to all followers concurrently using semi-synchronous replication.
    
    Semi-synchronous replication means:
    - We send replication requests to ALL followers concurrently
    - We wait until WRITE_QUORUM followers have confirmed
    - Once quorum is reached, we return success (remaining replications continue in background)
    
    Args:
        key: The key being written
        value: The value being written
        version: The version number of this write
    
    Returns:
        A dictionary with:
        - 'success': True if quorum was reached
        - 'confirmations': Number of successful confirmations
        - 'quorum_required': The configured write quorum
        - 'details': List of per-follower results
    
    Postconditions:
        - Returns when quorum is reached OR all futures complete/timeout
        - All replication attempts are made concurrently
    """
    if not FOLLOWERS:
        # No followers configured - write is trivially successful
        return {
            'success': True,
            'confirmations': 0,
            'quorum_required': WRITE_QUORUM,
            'details': []
        }
    
    # Submit replication tasks to all followers CONCURRENTLY
    futures = {}
    for follower_url in FOLLOWERS:
        future = replication_executor.submit(
            replicate_to_follower, 
            follower_url, 
            key, 
            value, 
            version
        )
        futures[future] = follower_url
    
    # Wait for confirmations until we reach quorum or all complete
    confirmations = 0
    results = []
    
    # Use as_completed to process results as they come in
    for future in as_completed(futures, timeout=REPLICATION_TIMEOUT * 2):
        try:
            result = future.result()
            results.append(result)
            
            if result['success']:
                confirmations += 1
                with stats_lock:
                    stats['replication_successes'] += 1
                
                # Check if we've reached quorum - can return early!
                if confirmations >= WRITE_QUORUM:
                    # Quorum reached - return success
                    # Remaining replications continue in background
                    return {
                        'success': True,
                        'confirmations': confirmations,
                        'quorum_required': WRITE_QUORUM,
                        'details': results
                    }
            else:
                with stats_lock:
                    stats['replication_failures'] += 1
        
        except Exception as e:
            results.append({
                'success': False, 
                'follower': futures[future], 
                'error': str(e)
            })
            with stats_lock:
                stats['replication_failures'] += 1
    
    # All futures completed - check if quorum was reached
    quorum_reached = confirmations >= WRITE_QUORUM
    
    return {
        'success': quorum_reached,
        'confirmations': confirmations,
        'quorum_required': WRITE_QUORUM,
        'details': results
    }


# =============================================================================
# HTTP API ENDPOINTS
# =============================================================================

@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint.
    
    Returns:
        JSON with role, status, and configuration info
    """
    return jsonify({
        'status': 'healthy',
        'role': 'leader',
        'followers': FOLLOWERS,
        'write_quorum': WRITE_QUORUM,
        'min_delay': MIN_DELAY,
        'max_delay': MAX_DELAY,
        'store_size': store.size(),
        'version': store.get_version()
    })


@app.route('/get/<key>', methods=['GET'])
def get_value(key: str):
    """
    Get the value for a key.
    
    Both leader and followers can serve reads (eventual consistency).
    
    Args:
        key: The key to retrieve (URL parameter)
    
    Returns:
        JSON with the key and value, or 404 if not found
    """
    value = store.get(key)
    
    if value is None:
        return jsonify({
            'error': 'Key not found',
            'key': key
        }), 404
    
    return jsonify({
        'key': key,
        'value': value,
        'version': store.get_version()
    })


@app.route('/set', methods=['POST'])
def set_value():
    """
    Set a key-value pair.
    
    This is the WRITE endpoint - only the leader accepts writes.
    The write is replicated to followers using semi-synchronous replication.
    
    Request body (JSON):
        - key: The key to set
        - value: The value to set
    
    Returns:
        JSON with write result and replication status
        - 200: Write successful (quorum reached)
        - 500: Write failed (quorum not reached)
    """
    data = request.get_json()
    
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({
            'error': 'Missing required fields: key and value'
        }), 400
    
    key = data['key']
    value = data['value']
    
    # Step 1: Write to local store first
    version = store.set(key, value)
    
    with stats_lock:
        stats['writes_total'] += 1
    
    # Step 2: Replicate to followers (semi-synchronous)
    replication_result = replicate_to_all_followers(key, value, version)
    
    if replication_result['success']:
        with stats_lock:
            stats['writes_successful'] += 1
        
        return jsonify({
            'success': True,
            'key': key,
            'value': value,
            'version': version,
            'replication': replication_result
        })
    else:
        with stats_lock:
            stats['writes_failed'] += 1
        
        # Note: The write is still in the local store even if replication failed
        # This is a design choice - we could also roll it back
        return jsonify({
            'success': False,
            'error': 'Write quorum not reached',
            'key': key,
            'version': version,
            'replication': replication_result
        }), 500


@app.route('/delete/<key>', methods=['DELETE'])
def delete_value(key: str):
    """
    Delete a key from the store.
    
    Note: For simplicity, delete operations are not replicated in this implementation.
    In a production system, deletes should also be replicated.
    
    Args:
        key: The key to delete (URL parameter)
    
    Returns:
        JSON with deletion status
    """
    deleted = store.delete(key)
    
    return jsonify({
        'success': deleted,
        'key': key
    })


@app.route('/all', methods=['GET'])
def get_all():
    """
    Get all key-value pairs in the store.
    
    Returns:
        JSON with all data and metadata
    """
    return jsonify({
        'data': store.get_all(),
        'size': store.size(),
        'version': store.get_version()
    })


@app.route('/keys', methods=['GET'])
def get_keys():
    """
    Get all keys in the store.
    
    Returns:
        JSON with list of keys
    """
    return jsonify({
        'keys': store.get_keys(),
        'count': store.size()
    })


@app.route('/stats', methods=['GET'])
def get_stats():
    """
    Get server statistics.
    
    Returns:
        JSON with write/replication statistics
    """
    with stats_lock:
        return jsonify({
            'role': 'leader',
            'stats': dict(stats),
            'store_size': store.size(),
            'version': store.get_version(),
            'config': {
                'write_quorum': WRITE_QUORUM,
                'min_delay': MIN_DELAY,
                'max_delay': MAX_DELAY,
                'followers': len(FOLLOWERS)
            }
        })


@app.route('/clear', methods=['POST'])
def clear_store():
    """
    Clear all data from the store (for testing purposes).
    
    WARNING: This is destructive and not replicated!
    """
    store.clear()
    return jsonify({
        'success': True,
        'message': 'Store cleared'
    })


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("LEADER NODE STARTING")
    print("=" * 60)
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print(f"Followers: {FOLLOWERS}")
    print(f"Write Quorum: {WRITE_QUORUM}")
    print(f"Min Delay: {MIN_DELAY}s ({MIN_DELAY * 1000:.2f}ms)")
    print(f"Max Delay: {MAX_DELAY}s ({MAX_DELAY * 1000:.2f}ms)")
    print(f"Replication Workers: {REPLICATION_WORKERS}")
    print("=" * 60)
    
    # Use threaded=True for concurrent request handling
    app.run(host=HOST, port=PORT, threaded=True)