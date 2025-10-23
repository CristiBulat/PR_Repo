#!/usr/bin/env python3
"""
Race Condition Testing Script
Tests whether the server's counter has race conditions.
"""
import socket
import threading
import time
import re
from datetime import datetime


def make_request(host, port, path, request_num, results):
    """Make a single HTTP GET request."""
    try:
        # Create socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(5)

        # Connect
        client_socket.connect((host, port))

        # Send HTTP GET request
        request = f"GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n"
        client_socket.sendall(request.encode('utf-8'))

        # Receive response
        response = b''
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            response += chunk

        client_socket.close()

        results[request_num] = {'success': True}

    except Exception as e:
        results[request_num] = {'success': False, 'error': str(e)}


def get_counter_value(host, port, target_dir_path, target_file):
    """Fetch the directory listing and extract counter for target file."""
    try:
        # Create socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(5)

        # Connect
        client_socket.connect((host, port))

        # Request directory listing for the /Books/ directory
        request = f"GET {target_dir_path} HTTP/1.1\r\nHost: {host}\r\n\r\n"
        client_socket.sendall(request.encode('utf-8'))

        # Receive full response
        response = b''
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            response += chunk

        client_socket.close()

        # Parse HTML to find counter value
        html = response.decode('utf-8', errors='ignore')

        # FIXED: Simpler pattern that handles icon spans and class names
        # Looks for: filename.pdf</a></td><td class="hits">123</td>
        pattern = rf'{re.escape(target_file)}</a></td><td class="hits">(\d+)</td>'
        match = re.search(pattern, html)

        if match:
            return int(match.group(1))
        else:
            print(f"Warning: Could not find counter for {target_file} in HTML")
            print("--- HTML Received ---")
            print(html)
            print("---------------------")
            return None

    except Exception as e:
        print(f"Error fetching counter: {e}")
        return None


def test_race_condition(host, port, target_path, num_requests):
    """Test for race conditions by making concurrent requests."""

    print(f"Target: http://{host}:{port}{target_path}")
    print(f"Number of requests: {num_requests}")

    results = {}
    threads = []

    # Record start time
    start_time = time.time()

    # Create and start all threads
    print(f"Sending {num_requests} concurrent requests to {target_path}...")
    for i in range(num_requests):
        thread = threading.Thread(
            target=make_request,
            args=(host, port, target_path, i + 1, results)
        )
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    end_time = time.time()
    elapsed = end_time - start_time

    # Check results
    successful = sum(1 for r in results.values() if r.get('success', False))
    failed = num_requests - successful

    print(f"Done! ({elapsed:.2f}s)")
    print(f"Successful requests: {successful}/{num_requests}")
    if failed > 0:
        print(f"Failed requests: {failed}")

    # Now fetch the counter value
    time.sleep(0.5)  # Brief pause to let server finish processing

    # Must split the path to check the directory
    target_dir_path = "/" + "/".join(target_path.lstrip("/").split("/")[:-1]) + "/"
    target_file_name = target_path.split("/")[-1]

    print(f"Fetching counter from directory: {target_dir_path}")
    counter_value = get_counter_value(host, port, target_dir_path, target_file_name)

    print(f"\n{'=' * 60}")
    print(f"Results")
    print(f"{'=' * 60}")
    print(f"Successful requests: {successful}")
    
    if counter_value is not None:
        print(f"Actual counter value: {counter_value}")
        print(f"\n{'=' * 60}")
        print(f"Analysis")
        print(f"{'=' * 60}")
        
        # Calculate lost updates
        lost_updates = num_requests - counter_value
        lost_percentage = (lost_updates / num_requests) * 100
        
        if lost_updates == 0:
            print(f"✅ THREAD-SAFE: Counter is perfect!")
            print(f"   All {num_requests} requests were counted correctly.")
            print(f"   No race condition detected.")
        else:
            print(f"❌ RACE CONDITION DETECTED!")
            print(f"   Expected counter: {num_requests}")
            print(f"   Actual counter: {counter_value}")
            print(f"   Lost updates: {lost_updates} ({lost_percentage:.1f}%)")
            print(f"\n   This demonstrates the race condition!")
            print(f"   Multiple threads read the same value, then all wrote back")
            print(f"   the same incremented value, losing concurrent updates.")
        
        print(f"\n{'=' * 60}")
        print(f"Interpretation")
        print(f"{'=' * 60}")
        print(f"• If SERVER_MODE = 'race': Counter will be < {num_requests} (race condition)")
        print(f"• If SERVER_MODE = 'threadsafe': Counter will be = {num_requests} (correct)")
        print(f"• If SERVER_MODE = 'ratelimit': Counter will be = {num_requests} (correct)")
    else:
        print("❌ Could not verify counter value from HTML.")
        print("   Check that the server is running in a mode that supports counters.")
        print("   Valid modes: 'race', 'threadsafe', 'ratelimit'")

    return counter_value


def main():
    host = '127.0.0.1'
    port = 8080
    target_file = '/Books/Chapter_1.pdf'
    num_requests = 100  # Number of concurrent requests

    print("=" * 60)
    print("Race Condition Testing Tool")
    print("=" * 60)
    print("\nThis test demonstrates race conditions in concurrent code.")
    print("\nWARNING: For best results, restart the server before running.")
    print("         This ensures the counter starts at 0.\n")
    print(f"Command: docker-compose restart\n")
    print("=" * 60)

    # Run the test
    counter = test_race_condition(host, port, target_file, num_requests)

if __name__ == '__main__':
    main()