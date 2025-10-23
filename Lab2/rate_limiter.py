"""
Thread-safe rate limiter for Lab 2.
Uses the "token bucket" algorithm (via a sliding window of timestamps).
"""

import time
import threading
from collections import defaultdict, deque

class RateLimiter:
    """
    A thread-safe rate limiter implementation.
    Tracks requests per client_id (e.g., IP address).
    """
    
    def __init__(self, limit, per_second):
        """
        Initialize the rate limiter.
        
        Args:
            limit (int): Max number of requests allowed.
            per_second (int): The time window in seconds.
        """
        self.limit = limit
        self.per_second = per_second
        # Use a defaultdict to store a deque (timestamp log) for each client
        self.clients = defaultdict(deque)
        # Use a single lock to protect access to the self.clients dictionary
        self.lock = threading.Lock()

    def allow(self, client_id):
        """
        Check if a request from client_id should be allowed.
        
        Args:
            client_id (str): A unique identifier for the client (e.g., IP).
            
        Returns:
            bool: True if the request is allowed, False if it is rate-limited.
        """
        current_time = time.time()
        
        # Lock to ensure thread-safety when modifying the client's timestamp deque
        with self.lock:
            # Get the timestamp log for this client
            timestamp_log = self.clients[client_id]
            
            # --- Prune old timestamps ---
            # Remove all timestamps older than the time window (current_time - per_second)
            # This is the "sliding window" part.
            while timestamp_log and timestamp_log[0] <= current_time - self.per_second:
                timestamp_log.popleft()
            
            # --- Check limit ---
            # If the number of timestamps in the log is still >= limit, deny request
            if len(timestamp_log) >= self.limit:
                return False
            
            # --- Allow and record ---
            # Otherwise, allow the request and add the new timestamp
            timestamp_log.append(current_time)
            return True