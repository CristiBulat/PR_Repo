"""
Lab 2 Race Condition Test Script
This script makes 100 concurrent requests to the *same file* to
test the thread-safe counter.
"""

import threading
import time
import subprocess
import sys

# --- Configuration ---
NUM_THREADS = 100  # Number of requests to make
TARGET_HOST = "localhost"
TARGET_PORT = "8080"
# Request a file that will trigger the counter
TARGET_PATH = "/Books/Chapter_1.pdf" 
DOWNLOAD_DIR = "./downloads"
CLIENT_SCRIPT = "client.py"
# ---------------------

print_lock = threading.Lock()
success_count = 0
success_lock = threading.Lock()

def make_request(request_num):
    """
    Function to be run by each thread.
    It calls the Lab 1 client.py as a subprocess.
    """
    global success_count
    thread_name = f"[Thread-{request_num:03d}]"
    
    command = [
        "python", 
        CLIENT_SCRIPT, 
        TARGET_HOST, 
        TARGET_PORT, 
        TARGET_PATH, 
        DOWNLOAD_DIR
    ]
    
    try:
        # Run the client.py script, hiding its output
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        
        with print_lock:
            if result.returncode == 0:
                print(f"✅ {thread_name} Request FINISHED")
                with success_lock:
                    success_count += 1
            else:
                print(f"❌ {thread_name} Request FAILED")
                print(result.stderr)

    except Exception as e:
        with print_lock:
            print(f"❌ {thread_name} FAILED ({e})")


def main():
    """
    Main function to spawn threads and check the counter.
    """
    print("--- Lab 2 Race Condition Test ---")
    print(f"Starting {NUM_THREADS} concurrent requests to {TARGET_PATH}")
    
    threads = []
    
    # Create and start all threads
    for i in range(NUM_THREADS):
        thread = threading.Thread(target=make_request, args=(i+1,))
        threads.append(thread)
        thread.start()
        time.sleep(0.005) # Stagger starts slightly
        
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
        
    print("\n--- Test Complete ---")
    print(f"{success_count} / {NUM_THREADS} requests were successful.")
    
    print("\nCheck your server's directory listing for '/Books/' in a browser.")
    print(f"The 'Hits' for 'Chapter_1.pdf' should be exactly {success_count}.")
    print("(If you ran this test before, the count will be {success_count} + previous_count).")
    
    if success_count != NUM_THREADS:
        print("\n[WARNING] Not all requests succeeded. Check server/client logs.")

if __name__ == "__main__":
    main()