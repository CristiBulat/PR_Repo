"""
Lab 2 Concurrency Test Script
This script makes 10 concurrent requests to the HTTP server and measures
the total time taken.
It re-uses the client.py from Lab 1.
"""

import threading
import time
import subprocess
import sys

# --- Configuration ---
NUM_REQUESTS = 10
TARGET_HOST = "localhost"
TARGET_PORT = "8080"
# Request a file that will trigger the 1s delay
TARGET_PATH = "/Dog.png" 
DOWNLOAD_DIR = "./downloads"
CLIENT_SCRIPT = "client.py"
# ---------------------

# A lock for printing to avoid jumbled terminal output
print_lock = threading.Lock()

def make_request(request_num):
    """
    Function to be run by each thread.
    It calls the Lab 1 client.py as a subprocess.
    """
    thread_name = f"[Thread-{request_num}]"
    
    command = [
        "python", 
        CLIENT_SCRIPT, 
        TARGET_HOST, 
        TARGET_PORT, 
        TARGET_PATH, 
        DOWNLOAD_DIR
    ]
    
    try:
        with print_lock:
            print(f"{thread_name} Starting request...")
        
        start_time = time.time()
        
        # Run the client.py script
        # capture_output=True hides the client's own print statements
        # text=True decodes stdout/stderr as text
        result = subprocess.run(
            command, 
            capture_output=True, 
            text=True, 
            timeout=10
        )
        
        end_time = time.time()
        
        with print_lock:
            if result.returncode == 0:
                print(f"✅ {thread_name} FINISHED in {end_time - start_time:.2f}s")
                # print(result.stdout) # Uncomment to see client output
            else:
                print(f"❌ {thread_name} FAILED after {end_time - start_time:.2f}s")
                print(result.stderr) # Print error if subprocess failed

    except subprocess.TimeoutExpired:
        with print_lock:
            print(f"❌ {thread_name} FAILED (Timeout)")
    except Exception as e:
        with print_lock:
            print(f"❌ {thread_name} FAILED ({e})")


def main():
    """
    Main function to spawn threads and measure time.
    """
    print("--- Lab 2 Concurrency Test ---")
    print(f"Starting {NUM_REQUESTS} concurrent requests to http://{TARGET_HOST}:{TARGET_PORT}{TARGET_PATH}")
    
    threads = []
    
    # --- Start Timer ---
    total_start_time = time.time()
    
    # Create and start all threads
    for i in range(NUM_REQUESTS):
        thread = threading.Thread(target=make_request, args=(i+1,))
        threads.append(thread)
        thread.start()
        # Small stagger to prevent all threads from starting at the *exact* same microsecond
        time.sleep(0.01) 
        
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
        
    # --- Stop Timer ---
    total_end_time = time.time()
    
    total_time = total_end_time - total_start_time
    
    print("\n--- Test Complete ---")
    print(f"All {NUM_REQUESTS} requests finished in: {total_time:.2f} seconds")
    
    if total_time < 2:
        print("\nResult: ⚡ CONCURRENT ⚡")
        print("Requests were handled in parallel (total time < 10s).")
    elif total_time > (NUM_REQUESTS * 0.9): # > 9s
        print("\nResult: 🐌 SINGLE-THREADED 🐌")
        print("Requests were handled one by one (total time ≈ 10s).")
    else:
        print(f"\nResult: 🤔 Partial Concurrency?")
        print(f"Time ({total_time:.2f}s) is between 2s and 9s. Check server load.")

if __name__ == "__main__":
    main()