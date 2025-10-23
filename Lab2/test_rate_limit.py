"""
Lab 2 Rate Limit Test Script
This script makes 20 requests in a rapid burst to test
the 5 req/s rate limiter.
"""

import threading
import time
import subprocess
import sys

# --- Configuration ---
NUM_REQUESTS = 20  # Total requests to send in a burst
TARGET_HOST = "localhost"
TARGET_PORT = "8080"
TARGET_PATH = "/" 
DOWNLOAD_DIR = "./downloads"
CLIENT_SCRIPT = "client.py"
# ---------------------

print_lock = threading.Lock()
results = {"200 OK": 0, "429 Blocked": 0, "Other": 0}
results_lock = threading.Lock()

def make_request(request_num):
    """
    Function to be run by each thread.
    It calls the Lab 1 client.py as a subprocess.
    """
    thread_name = f"[Thread-{request_num:02d}]"
    
    command = [
        "python", 
        CLIENT_SCRIPT, 
        TARGET_HOST, 
        TARGET_PORT, 
        TARGET_PATH, 
        DOWNLOAD_DIR
    ]
    
    try:
        # Run the client.py script, capturing its output
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        
        with print_lock:
            if "RESPONSE] 200 OK" in result.stdout:
                print(f"✅ {thread_name} Allowed (200 OK)")
                with results_lock:
                    results["200 OK"] += 1
            elif "RESPONSE] 429 Too Many Requests" in result.stdout:
                print(f"❌ {thread_name} Blocked (429 Too Many Requests)")
                with results_lock:
                    results["429 Blocked"] += 1
            else:
                print(f"❓ {thread_name} Other Response\n{result.stderr}")
                with results_lock:
                    results["Other"] += 1

    except Exception as e:
        with print_lock:
            print(f"❓ {thread_name} FAILED ({e})")
            with results_lock:
                results["Other"] += 1


def main():
    """
    Main function to spawn threads and check rate limit results.
    """
    print("--- Lab 2 Rate Limit Test ---")
    print(f"Starting {NUM_REQUESTS} requests in a rapid burst (expecting ~5 OK, ~15 Blocked)...")
    
    threads = []
    
    # Create and start all threads very quickly
    for i in range(NUM_REQUESTS):
        thread = threading.Thread(target=make_request, args=(i+1,))
        threads.append(thread)
        thread.start()
        time.sleep(0.02) # Send 50 req/s, much faster than the 5 req/s limit
        
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
        
    print("\n--- Test Complete ---")
    print("Rate Limit Results:")
    print(f"  Allowed (200 OK):      {results['200 OK']}")
    print(f"  Blocked (429 Blocked): {results['429 Blocked']}")
    print(f"  Other/Failed:        {results['Other']}")
    
    if 4 <= results['200 OK'] <= 7 and results['429 Blocked'] > 10:
        print("\n[SUCCESS] Rate limiter appears to be working correctly.")
    else:
        print("\n[WARNING] Results are not as expected. Check server/limiter logic.")

if __name__ == "__main__":
    main()