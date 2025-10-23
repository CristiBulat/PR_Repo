#!/usr/bin/env python3
"""
Rate Limiting Test Script
Tests that the server enforces ~5 requests/second per IP.
"""
import socket
import time
import threading


def make_request(host, port, path, request_num, delay=0):
    """Make a single HTTP GET request with optional delay."""
    if delay > 0:
        time.sleep(delay)

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(3)
        client_socket.connect((host, port))

        request = f"GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n"
        client_socket.sendall(request.encode('utf-8'))

        response = b''
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            response += chunk

        client_socket.close()

        # Parse status code
        status_line = response.split(b'\r\n')[0].decode('utf-8', errors='ignore')
        status_code = status_line.split()[1] if len(status_line.split()) > 1 else 'Unknown'

        return {'success': True, 'status': int(status_code)}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def test_rapid_requests(host, port, path, num_requests):
    """Test with rapid concurrent requests (should hit rate limit)."""
    print(f"\n{'=' * 60}")
    print(f"TEST 1: RAPID REQUESTS")
    print(f"{'=' * 60}")
    print(f"Sending {num_requests} requests as fast as possible...")

    start_time = time.time()
    results = []
    threads = []

    # Send all requests at once
    for i in range(num_requests):
        result_container = [None]
        thread = threading.Thread(
            target=lambda r=result_container, n=i: r.__setitem__(0, make_request(host, port, path, n))
        )
        results.append(result_container)
        threads.append(thread)
        thread.start()
        time.sleep(0.02) # Stagger slightly to simulate a burst

    for thread in threads:
        thread.join()

    elapsed = time.time() - start_time

    # Analyze results
    results = [r[0] for r in results if r[0] is not None]
    status_200 = sum(1 for r in results if r.get('status') == 200)
    status_429 = sum(1 for r in results if r.get('status') == 429)
    failed = sum(1 for r in results if not r.get('success', False))

    print(f"\nCompleted in {elapsed:.2f}s")
    print(f"Results:")
    print(f"  - 200 OK: {status_200}")
    print(f"  - 429 Too Many Requests: {status_429}")
    print(f"  - Failed: {failed}")
    
    if 4 <= status_200 <= 7 and status_429 > 10:
        print("\n[SUCCESS] Rate limiter appears to be working correctly.")
    else:
        print("\n[WARNING] Results are not as expected. Check server/limiter logic.")


def test_slow_requests(host, port, path, num_requests, delay_between):
    """Test with slow sequential requests (should all succeed)."""
    print(f"\n{'=' * 60}")
    print(f"TEST 2: SLOW REQUESTS")
    print(f"{'=' * 60}")
    print(f"Sending {num_requests} requests with {delay_between}s delay...")

    start_time = time.time()
    results = []

    for i in range(num_requests):
        if i > 0:
            time.sleep(delay_between)
        result = make_request(host, port, path, i)
        results.append(result)
        status = result.get('status', 'Error')
        print(f"  Request {i + 1}: {status}")

    elapsed = time.time() - start_time

    # Analyze results
    status_200 = sum(1 for r in results if r.get('status') == 200)
    status_429 = sum(1 for r in results if r.get('status') == 429)
    failed = sum(1 for r in results if not r.get('success', False))

    print(f"\nCompleted in {elapsed:.2f}s")
    print(f"Results:")
    print(f"  - 200 OK: {status_200}")
    print(f"  - 429 Too Many Requests: {status_429}")
    print(f"  - Failed: {failed}")

    if status_200 == num_requests and status_429 == 0:
        print("\n[SUCCESS] All slow requests were allowed, as expected.")
    else:
        print("\n[WARNING] Slow requests were blocked or failed. This is unexpected.")


def main():
    host = '127.0.0.1'
    # Use your port 8080
    port = 8080
    # Use a file from your collection
    path = '/Books/Chapter_1.pdf'  # Target file

    print(f"\nTesting server at http://{host}:{port}")
    print(f"Rate limit: ~5 requests/second per IP")

    # Test 1: Rapid requests (should hit limit)
    test_rapid_requests(host, port, path, num_requests=20)

    # Wait a bit between tests
    time.sleep(2)

    # Test 2: Slow requests (should succeed)
    test_slow_requests(host, port, path, num_requests=10, delay_between=0.25)

    print(f"\n{'=' * 60}")
    print("Testing Complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()