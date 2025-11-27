"""
Follower Node - Lab 4
Implements the follower node for the distributed key-value store.

The follower:
- Accepts read requests from clients (eventual consistency)
- Receives replicated writes from the leader
- Does NOT accept direct write requests from clients
- Executes all requests concurrently
"""

import os
import sys
from flask import Flask, request, jsonify
from kv_store import KeyValueStore
from datetime import datetime
import threading

# =============================================================================
# CONFIGURATION - Read from environment variables
# =============================================================================

# Server configuration
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', 5001))

# Node identifier (for debugging and identification)
NODE_ID = os.environ.get('NODE_ID', f'follower-{PORT}')

# Leader URL (for potential read-your-writes or leader forwarding)
LEADER_URL = os.environ.get('LEADER_URL', 'http://leader:5000')

# =============================================================================
# INITIALIZATION
# =============================================================================

app = Flask(__name__)
store = KeyValueStore()

# Statistics tracking
stats = {
    'reads_total': 0,
    'replications_received': 0,
    'replications_applied': 0,
    'write_rejections': 0,
    'start_time': datetime.utcnow().isoformat()
}
stats_lock = threading.Lock()


# =============================================================================
# HTTP API ENDPOINTS
# =============================================================================

@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint.
    
    Returns:
        JSON with role, status, and node info
    """
    return jsonify({
        'status': 'healthy',
        'role': 'follower',
        'node_id': NODE_ID,
        'leader_url': LEADER_URL,
        'store_size': store.size(),
        'version': store.get_version()
    })


@app.route('/get/<key>', methods=['GET'])
def get_value(key: str):
    """
    Get the value for a key.
    
    Followers can serve reads (eventual consistency model).
    The data may be slightly stale if replication is in progress.
    
    Args:
        key: The key to retrieve (URL parameter)
    
    Returns:
        JSON with the key and value, or 404 if not found
    
    Postconditions:
        - Returns the value if found
        - Increments read counter
    """
    with stats_lock:
        stats['reads_total'] += 1
    
    value = store.get(key)
    
    if value is None:
        return jsonify({
            'error': 'Key not found',
            'key': key,
            'node': NODE_ID
        }), 404
    
    return jsonify({
        'key': key,
        'value': value,
        'version': store.get_version(),
        'node': NODE_ID
    })


@app.route('/set', methods=['POST'])
def set_value():
    """
    Reject direct write requests.
    
    Followers do NOT accept writes - all writes must go through the leader.
    This is the single-leader replication model.
    
    Returns:
        403 Forbidden with instruction to use the leader
    """
    with stats_lock:
        stats['write_rejections'] += 1
    
    return jsonify({
        'error': 'Writes not allowed on follower',
        'message': 'Please send write requests to the leader',
        'leader_url': LEADER_URL,
        'node': NODE_ID
    }), 403


@app.route('/replicate', methods=['POST'])
def replicate():
    """
    Receive a replicated write from the leader.
    
    This is the internal API used by the leader to replicate writes.
    It applies the write to the local store.
    
    Request body (JSON):
        - key: The key to set
        - value: The value to set
        - version: The leader's version number for this write
    
    Returns:
        JSON with replication status
        - 200: Replication successful
        - 400: Invalid request
    
    Preconditions:
        - Request comes from the leader
        - Request body contains key, value, and version
    
    Postconditions:
        - Write is applied to local store
        - Local version is updated
        - Stats are updated
    """
    with stats_lock:
        stats['replications_received'] += 1
    
    data = request.get_json()
    
    if not data:
        return jsonify({
            'error': 'Missing request body',
            'node': NODE_ID
        }), 400
    
    # Validate required fields
    required_fields = ['key', 'value', 'version']
    for field in required_fields:
        if field not in data:
            return jsonify({
                'error': f'Missing required field: {field}',
                'node': NODE_ID
            }), 400
    
    key = data['key']
    value = data['value']
    version = data['version']
    
    # Apply the replicated write to local store
    success = store.apply_write(key, value, version)
    
    if success:
        with stats_lock:
            stats['replications_applied'] += 1
        
        return jsonify({
            'success': True,
            'key': key,
            'version': store.get_version(),
            'node': NODE_ID
        })
    else:
        return jsonify({
            'success': False,
            'error': 'Failed to apply replication',
            'node': NODE_ID
        }), 500


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
        'version': store.get_version(),
        'node': NODE_ID
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
        'count': store.size(),
        'node': NODE_ID
    })


@app.route('/stats', methods=['GET'])
def get_stats():
    """
    Get server statistics.
    
    Returns:
        JSON with read/replication statistics
    """
    with stats_lock:
        return jsonify({
            'role': 'follower',
            'node_id': NODE_ID,
            'stats': dict(stats),
            'store_size': store.size(),
            'version': store.get_version()
        })


@app.route('/clear', methods=['POST'])
def clear_store():
    """
    Clear all data from the store (for testing purposes).
    
    WARNING: This is destructive! Use with caution.
    """
    store.clear()
    return jsonify({
        'success': True,
        'message': 'Store cleared',
        'node': NODE_ID
    })


@app.route('/compare', methods=['GET'])
def compare_with_leader():
    """
    Compare this follower's data with the leader (for testing).
    
    Returns data that can be used to verify replication consistency.
    
    Returns:
        JSON with store data for comparison
    """
    return jsonify({
        'node': NODE_ID,
        'role': 'follower',
        'data': store.get_all(),
        'size': store.size(),
        'version': store.get_version()
    })


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("FOLLOWER NODE STARTING")
    print("=" * 60)
    print(f"Node ID: {NODE_ID}")
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print(f"Leader URL: {LEADER_URL}")
    print("=" * 60)
    
    # Use threaded=True for concurrent request handling
    # This is REQUIRED by the lab - "Both leader and followers should execute all requests concurrently"
    app.run(host=HOST, port=PORT, threaded=True)