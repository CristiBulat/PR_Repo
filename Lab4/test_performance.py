#!/usr/bin/env python3
"""
Performance Analysis - Lab 4
Analyzes the distributed key-value store performance.

This script:
1. Makes ~10K writes concurrently (>10 threads) on 100 keys
2. Plots write quorum vs average latency
3. Verifies data consistency between leader and replicas

Requirements:
- matplotlib for plotting (pip install matplotlib)
- The system running with different WRITE_QUORUM values

Run with: python test_performance.py [--quorum N]
"""

import requests
import time
import sys
import threading
import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Any
from datetime import datetime, timezone
import statistics


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

# Test parameters (as per lab requirements)
NUM_KEYS = 100           # Number of unique keys to use
NUM_WRITES = 10000       # Total number of write operations (~10K)
NUM_THREADS = 20         # Number of concurrent threads (>10)

# Request timeout
TIMEOUT = 10.0


# =============================================================================
# UTILITIES
# =============================================================================

def wait_for_services(max_retries: int = 30) -> bool:
    """Wait for all services to be ready."""
    print("Waiting for services...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{LEADER_URL}/health", timeout=2)
            if response.status_code == 200:
                print("Services ready!")
                return True
        except:
            pass
        time.sleep(1)
    
    return False


def clear_all_stores() -> None:
    """Clear data from all nodes."""
    try:
        requests.post(f"{LEADER_URL}/clear", timeout=TIMEOUT)
        for url in FOLLOWER_URLS:
            requests.post(f"{url}/clear", timeout=TIMEOUT)
        time.sleep(0.5)
    except:
        pass


def get_current_quorum() -> int:
    """Get the current write quorum from the leader."""
    try:
        response = requests.get(f"{LEADER_URL}/health", timeout=TIMEOUT)
        return response.json().get('write_quorum', 1)
    except:
        return -1


# =============================================================================
# WRITE WORKER
# =============================================================================

def write_key(key: str, value: Any) -> Dict:
    """
    Perform a single write operation and measure latency.
    
    Args:
        key: The key to write
        value: The value to write
    
    Returns:
        Dictionary with success status and latency
    """
    start_time = time.time()
    
    try:
        response = requests.post(
            f"{LEADER_URL}/set",
            json={'key': key, 'value': value},
            timeout=TIMEOUT
        )
        
        end_time = time.time()
        latency = end_time - start_time
        
        return {
            'success': response.status_code == 200,
            'latency': latency,
            'status_code': response.status_code,
            'key': key
        }
    
    except Exception as e:
        end_time = time.time()
        latency = end_time - start_time
        
        return {
            'success': False,
            'latency': latency,
            'error': str(e),
            'key': key
        }


# =============================================================================
# PERFORMANCE TEST
# =============================================================================

def run_performance_test(num_writes: int = NUM_WRITES, 
                         num_keys: int = NUM_KEYS,
                         num_threads: int = NUM_THREADS) -> Dict:
    """
    Run the performance test with concurrent writes.
    
    Args:
        num_writes: Total number of write operations
        num_keys: Number of unique keys to use
        num_threads: Number of concurrent threads
    
    Returns:
        Dictionary with test results and statistics
    """
    print(f"\n{'=' * 60}")
    print("PERFORMANCE TEST")
    print("=" * 60)
    
    quorum = get_current_quorum()
    print(f"Current write quorum: {quorum}")
    print(f"Total writes: {num_writes}")
    print(f"Unique keys: {num_keys}")
    print(f"Concurrent threads: {num_threads}")
    print("-" * 60)
    
    # Clear stores before test
    clear_all_stores()
    
    # Generate write tasks
    # Distribute writes across keys evenly
    writes_per_key = num_writes // num_keys
    extra_writes = num_writes % num_keys
    
    tasks = []
    for i in range(num_keys):
        key = f"key_{i:03d}"
        count = writes_per_key + (1 if i < extra_writes else 0)
        for j in range(count):
            value = {
                'key_id': i,
                'write_id': j,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            tasks.append((key, value))
    
    print(f"Generated {len(tasks)} write tasks")
    
    # Execute writes concurrently
    print(f"Starting concurrent writes with {num_threads} threads...")
    
    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all write tasks
        futures = {
            executor.submit(write_key, key, value): (key, value)
            for key, value in tasks
        }
        
        # Collect results with progress
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            completed += 1
            if completed % 1000 == 0:
                print(f"  Progress: {completed}/{len(tasks)} ({100*completed/len(tasks):.1f}%)")
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Calculate statistics
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    latencies = [r['latency'] for r in successful]
    
    stats = {
        'quorum': quorum,
        'total_writes': len(tasks),
        'successful_writes': len(successful),
        'failed_writes': len(failed),
        'total_time': total_time,
        'throughput': len(successful) / total_time if total_time > 0 else 0,
        'avg_latency': statistics.mean(latencies) if latencies else 0,
        'median_latency': statistics.median(latencies) if latencies else 0,
        'min_latency': min(latencies) if latencies else 0,
        'max_latency': max(latencies) if latencies else 0,
        'std_latency': statistics.stdev(latencies) if len(latencies) > 1 else 0,
        'p95_latency': sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0,
        'p99_latency': sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0,
    }
    
    # Print results
    print("\n" + "-" * 60)
    print("RESULTS")
    print("-" * 60)
    print(f"Write Quorum: {stats['quorum']}")
    print(f"Total Time: {stats['total_time']:.2f}s")
    print(f"Successful Writes: {stats['successful_writes']}/{stats['total_writes']}")
    print(f"Failed Writes: {stats['failed_writes']}")
    print(f"Throughput: {stats['throughput']:.2f} writes/sec")
    print(f"\nLatency Statistics (seconds):")
    print(f"  Average: {stats['avg_latency']*1000:.2f}ms")
    print(f"  Median:  {stats['median_latency']*1000:.2f}ms")
    print(f"  Min:     {stats['min_latency']*1000:.2f}ms")
    print(f"  Max:     {stats['max_latency']*1000:.2f}ms")
    print(f"  Std Dev: {stats['std_latency']*1000:.2f}ms")
    print(f"  P95:     {stats['p95_latency']*1000:.2f}ms")
    print(f"  P99:     {stats['p99_latency']*1000:.2f}ms")
    
    return stats


# =============================================================================
# CONSISTENCY CHECK
# =============================================================================

def check_consistency() -> Dict:
    """
    Check if data in replicas matches the leader.
    
    Returns:
        Dictionary with consistency check results
    """
    print(f"\n{'=' * 60}")
    print("CONSISTENCY CHECK")
    print("=" * 60)
    
    try:
        # Get leader data
        leader_response = requests.get(f"{LEADER_URL}/all", timeout=TIMEOUT)
        leader_data = leader_response.json()
        leader_store = leader_data.get('data', {})
        leader_size = len(leader_store)
        leader_version = leader_data.get('version', 0)
        
        print(f"Leader: {leader_size} keys, version {leader_version}")
        
        # Compare with followers
        results = {
            'leader_size': leader_size,
            'leader_version': leader_version,
            'followers': [],
            'all_consistent': True
        }
        
        for i, url in enumerate(FOLLOWER_URLS, 1):
            try:
                follower_response = requests.get(f"{url}/all", timeout=TIMEOUT)
                follower_data = follower_response.json()
                follower_store = follower_data.get('data', {})
                follower_size = len(follower_store)
                follower_version = follower_data.get('version', 0)
                
                # Check for missing keys
                missing_keys = set(leader_store.keys()) - set(follower_store.keys())
                extra_keys = set(follower_store.keys()) - set(leader_store.keys())
                
                # Check for value mismatches
                mismatches = []
                for key in leader_store:
                    if key in follower_store:
                        if leader_store[key] != follower_store[key]:
                            mismatches.append(key)
                
                is_consistent = (
                    len(missing_keys) == 0 and 
                    len(extra_keys) == 0 and 
                    len(mismatches) == 0
                )
                
                status = "✅ CONSISTENT" if is_consistent else "❌ INCONSISTENT"
                print(f"Follower {i}: {follower_size} keys, version {follower_version} - {status}")
                
                if not is_consistent:
                    results['all_consistent'] = False
                    if missing_keys:
                        print(f"  Missing {len(missing_keys)} keys")
                    if extra_keys:
                        print(f"  Extra {len(extra_keys)} keys")
                    if mismatches:
                        print(f"  {len(mismatches)} value mismatches")
                
                results['followers'].append({
                    'id': i,
                    'size': follower_size,
                    'version': follower_version,
                    'consistent': is_consistent,
                    'missing_keys': len(missing_keys),
                    'extra_keys': len(extra_keys),
                    'mismatches': len(mismatches)
                })
            
            except Exception as e:
                print(f"Follower {i}: ERROR - {e}")
                results['followers'].append({
                    'id': i,
                    'error': str(e),
                    'consistent': False
                })
                results['all_consistent'] = False
        
        # Summary
        print("\n" + "-" * 60)
        if results['all_consistent']:
            print("✅ ALL REPLICAS ARE CONSISTENT WITH LEADER")
        else:
            print("❌ SOME REPLICAS ARE INCONSISTENT")
            print("\nExplanation: With semi-synchronous replication and concurrent writes,")
            print("it's possible for replicas to temporarily have different data.")
            print("This is expected behavior - eventual consistency is guaranteed.")
        
        return results
    
    except Exception as e:
        print(f"Error checking consistency: {e}")
        return {'error': str(e), 'all_consistent': False}


# =============================================================================
# QUORUM COMPARISON (Requires running multiple times with different quorum)
# =============================================================================

def save_results(stats: Dict, filename: str = "results.json") -> None:
    """Save test results to a JSON file."""
    try:
        # Load existing results
        try:
            with open(filename, 'r') as f:
                all_results = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            all_results = []
        
        # Add timestamp
        stats['timestamp'] = datetime.now(timezone.utc).isoformat()
        all_results.append(stats)
        
        # Save
        with open(filename, 'w') as f:
            json.dump(all_results, f, indent=2)
        
        print(f"\nResults saved to {filename}")
    except Exception as e:
        print(f"Error saving results: {e}")


def plot_quorum_comparison(filename: str = "results.json") -> None:
    """
    Plot write quorum vs latency and write quorum vs data consistency.

    Run the performance test multiple times with different WRITE_QUORUM values,
    then call this function to generate the comparison plots.
    """
    try:
        import matplotlib.pyplot as plt

        # Load results
        with open(filename, 'r') as f:
            results = json.load(f)

        if not results:
            print("No results to plot!")
            return

        # Group by quorum (take latest result for each quorum)
        quorum_data = {}
        for r in results:
            q = r.get('quorum', 0)
            quorum_data[q] = r  # Overwrite with latest

        # Sort by quorum
        quorums = sorted(quorum_data.keys())

        # Extract latency metrics (in seconds)
        mean_latencies = [quorum_data[q]['avg_latency'] for q in quorums]
        median_latencies = [quorum_data[q]['median_latency'] for q in quorums]
        p95_latencies = [quorum_data[q]['p95_latency'] for q in quorums]
        p99_latencies = [quorum_data[q]['p99_latency'] for q in quorums]

        # Extract consistency metrics
        failed_writes = [quorum_data[q]['failed_writes'] for q in quorums]
        total_mismatches = []
        for q in quorums:
            consistency = quorum_data[q].get('consistency', {})
            followers = consistency.get('followers', [])
            mismatches = sum(f.get('mismatches', 0) for f in followers)
            total_mismatches.append(mismatches)

        # =====================================================================
        # PLOT 1: Write Quorum vs Latency (line plot with actual test data)
        # =====================================================================
        fig1, ax1 = plt.subplots(figsize=(10, 6))

        x_labels = [f'Q={q}' for q in quorums]
        x_pos = range(len(quorums))

        # Use actual test data directly
        ax1.plot(x_pos, mean_latencies, 'b-o', label='mean', linewidth=2, markersize=6)
        ax1.plot(x_pos, median_latencies, color='orange', marker='o', linestyle='-', label='median', linewidth=2, markersize=6)
        ax1.plot(x_pos, p95_latencies, 'g-o', label='p95', linewidth=2, markersize=6)
        ax1.plot(x_pos, p99_latencies, 'r-o', label='p99', linewidth=2, markersize=6)

        ax1.set_xlabel('Quorum value', fontsize=12)
        ax1.set_ylabel('Latency (s)', fontsize=12)
        ax1.set_title('Quorum vs. Latency, follower delays [50ms-250ms]', fontsize=14)
        ax1.set_xticks(x_pos)
        ax1.set_xticklabels(x_labels)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('quorum_vs_latency.png', dpi=150)
        print("Plot saved to quorum_vs_latency.png")
        plt.close()

        # =====================================================================
        # PLOT 2: Write Quorum vs Data Consistency
        # =====================================================================
        fig2, ax2 = plt.subplots(figsize=(10, 6))

        # Create bar chart for failed writes and mismatches
        bar_width = 0.35
        x_pos = range(len(quorums))

        bars1 = ax2.bar([x - bar_width/2 for x in x_pos], failed_writes, bar_width,
                        label='Failed Writes', color='#e74c3c')
        bars2 = ax2.bar([x + bar_width/2 for x in x_pos], total_mismatches, bar_width,
                        label='Data Mismatches', color='#3498db')

        ax2.set_xlabel('Quorum value', fontsize=12)
        ax2.set_ylabel('Count', fontsize=12)
        ax2.set_title('Write Quorum vs. Data Consistency', fontsize=14)
        ax2.set_xticks(x_pos)
        ax2.set_xticklabels(x_labels)
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')

        # Add value labels on bars
        for bar in bars1:
            height = bar.get_height()
            ax2.annotate(f'{int(height)}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=10)
        for bar in bars2:
            height = bar.get_height()
            ax2.annotate(f'{int(height)}',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=10)

        plt.tight_layout()
        plt.savefig('quorum_vs_consistency.png', dpi=150)
        print("Plot saved to quorum_vs_consistency.png")
        plt.close()

        print("\nBoth plots generated successfully!")

    except ImportError:
        print("matplotlib not installed. Install with: pip install matplotlib")
    except FileNotFoundError:
        print(f"No results file found: {filename}")
        print("Run performance tests first to generate data.")
    except Exception as e:
        print(f"Error plotting: {e}")


def print_quorum_analysis() -> None:
    """Print analysis of quorum impact (text-based for reports)."""
    print(f"\n{'=' * 60}")
    print("QUORUM ANALYSIS EXPLANATION")
    print("=" * 60)
    print("""
As the write quorum increases from 1 to 5, we expect to see:

1. LATENCY INCREASES:
   - With quorum=1: Leader waits for just 1 follower (fastest response wins)
   - With quorum=5: Leader waits for ALL 5 followers (slowest response matters)
   - Network delays are random, so higher quorum = higher expected latency

2. THROUGHPUT DECREASES:
   - Higher latency means fewer writes can be processed per second
   - Threads spend more time waiting for quorum confirmation

3. DURABILITY INCREASES:
   - Higher quorum = more replicas have the data before write succeeds
   - Quorum=5 means all replicas have data (strongest durability guarantee)

4. AVAILABILITY TRADEOFF:
   - Quorum=1: Can succeed even if 4 followers are slow/down
   - Quorum=5: Fails if any single follower is unavailable

To test this, run the performance test with different WRITE_QUORUM values:

    # In docker-compose.yml, change WRITE_QUORUM and restart:
    docker-compose down
    # Edit docker-compose.yml to set WRITE_QUORUM=N (1, 2, 3, 4, or 5)
    docker-compose up -d
    python test_performance.py
""")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Performance test for distributed KV store')
    parser.add_argument('--writes', type=int, default=NUM_WRITES,
                        help=f'Number of writes (default: {NUM_WRITES})')
    parser.add_argument('--keys', type=int, default=NUM_KEYS,
                        help=f'Number of unique keys (default: {NUM_KEYS})')
    parser.add_argument('--threads', type=int, default=NUM_THREADS,
                        help=f'Number of threads (default: {NUM_THREADS})')
    parser.add_argument('--plot', action='store_true',
                        help='Plot comparison from saved results')
    parser.add_argument('--explain', action='store_true',
                        help='Print quorum analysis explanation')
    args = parser.parse_args()
    
    if args.explain:
        print_quorum_analysis()
        return
    
    if args.plot:
        plot_quorum_comparison()
        return
    
    # Wait for services
    if not wait_for_services():
        print("Services not available. Run 'docker-compose up' first!")
        sys.exit(1)
    
    # Run performance test
    stats = run_performance_test(
        num_writes=args.writes,
        num_keys=args.keys,
        num_threads=args.threads
    )
    
    # Check consistency
    consistency = check_consistency()
    
    # Save results
    stats['consistency'] = consistency
    save_results(stats)
    
    # Print analysis
    print_quorum_analysis()


if __name__ == '__main__':
    main()