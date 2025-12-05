"""
Key-Value Store Module - Lab 4
A thread-safe key-value store implementation for the distributed system.
Supports concurrent read and write operations using locks.
"""

import threading
from typing import Dict, Optional, Any
from datetime import datetime, timezone


class KeyValueStore:
    """
    Thread-safe Key-Value Store implementation.
    
    Uses a dictionary to store key-value pairs with a read-write lock
    for concurrent access. All operations are atomic.
    
    Rep Invariant:
        - self._store is always a dictionary
        - self._lock is always a threading.RLock (reentrant lock)
        - Keys are always strings
        - Values can be any JSON-serializable type
    
    Safety from Rep Exposure:
        - _store is private and never returned directly
        - get_all() returns a copy of the store
        - All access is through public methods with locking
    """
    
    def __init__(self):
        """Initialize an empty key-value store with a lock for thread safety."""
        self._store: Dict[str, Any] = {}
        self._lock = threading.RLock()  # Reentrant lock for nested operations
        self._version = 0  # Monotonically increasing version for conflict detection
        self._write_log: list = []  # Log of all writes for debugging
    
    def _check_rep(self) -> None:
        """Verify the representation invariant holds."""
        assert isinstance(self._store, dict), "Store must be a dictionary"
        assert isinstance(self._lock, type(threading.RLock())), "Lock must be RLock"
        for key in self._store:
            assert isinstance(key, str), f"Key {key} must be a string"
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get the value associated with a key.
        
        Args:
            key: The key to look up (must be a non-empty string)
        
        Returns:
            The value if key exists, None otherwise
        
        Preconditions:
            - key is a non-empty string
        
        Postconditions:
            - Returns the value associated with key, or None if not found
            - Store state is unchanged
        """
        with self._lock:
            return self._store.get(key)
    
    def set(self, key: str, value: Any) -> int:
        """
        Set a key-value pair in the store.
        
        Args:
            key: The key to set (must be a non-empty string)
            value: The value to associate with the key (must be JSON-serializable)
        
        Returns:
            The per-key version number after the write (for replication ordering)
        
        Preconditions:
            - key is a non-empty string
            - value is JSON-serializable
        
        Postconditions:
            - Store contains the key-value pair
            - Global and per-key versions are incremented
            - Write is logged
        """
        with self._lock:
            # Track per-key versions for replication ordering
            if not hasattr(self, '_key_versions'):
                self._key_versions: Dict[str, int] = {}
            
            self._store[key] = value
            self._version += 1
            
            # Increment per-key version (this is what we send to followers)
            self._key_versions[key] = self._key_versions.get(key, 0) + 1
            key_version = self._key_versions[key]
            
            self._write_log.append({
                'version': self._version,
                'key_version': key_version,
                'key': key,
                'value': value,
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
            return key_version  # Return per-key version for replication
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from the store.
        
        Args:
            key: The key to delete
        
        Returns:
            True if key was deleted, False if key didn't exist
        
        Preconditions:
            - key is a string
        
        Postconditions:
            - Key no longer exists in store
            - Returns True if key existed, False otherwise
        """
        with self._lock:
            if key in self._store:
                del self._store[key]
                self._version += 1
                return True
            return False
    
    def get_all(self) -> Dict[str, Any]:
        """
        Get a copy of all key-value pairs in the store.
        
        Returns:
            A dictionary copy of all key-value pairs
        
        Postconditions:
            - Returns a shallow copy of the store
            - Original store is unaffected
        """
        with self._lock:
            return dict(self._store)
    
    def get_version(self) -> int:
        """
        Get the current version of the store.
        
        Returns:
            The current version number
        """
        with self._lock:
            return self._version
    
    def get_keys(self) -> list:
        """
        Get all keys in the store.
        
        Returns:
            A list of all keys
        """
        with self._lock:
            return list(self._store.keys())
    
    def size(self) -> int:
        """
        Get the number of key-value pairs in the store.
        
        Returns:
            The count of entries in the store
        """
        with self._lock:
            return len(self._store)
    
    def apply_write(self, key: str, value: Any, version: int) -> bool:
        """
        Apply a replicated write from the leader.
        Used by followers to apply writes received from the leader.
        
        Uses per-key versioning to handle out-of-order replication.
        Only applies the write if it's newer than the current value for that key.
        
        Args:
            key: The key to set
            value: The value to set
            version: The version from the leader (used as ordering)
        
        Returns:
            True if write was applied successfully
        
        Postconditions:
            - Store contains the key-value pair (if version is newer)
            - Local version is updated to match leader's version
        """
        with self._lock:
            # Track per-key versions to handle out-of-order replication
            if not hasattr(self, '_key_versions'):
                self._key_versions: Dict[str, int] = {}
            
            # Only apply if this version is newer than what we have for this key
            current_key_version = self._key_versions.get(key, 0)
            
            if version > current_key_version:
                self._store[key] = value
                self._key_versions[key] = version
                self._version = max(self._version, version)
                return True
            else:
                # Skip older write (out-of-order replication)
                return True  # Still return True - not an error, just skipped
    
    def clear(self) -> None:
        """Clear all data from the store (for testing purposes)."""
        with self._lock:
            self._store.clear()
            self._version = 0
            self._write_log.clear()