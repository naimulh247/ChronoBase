import mmap
import os
import json
import threading
import time
from collections import defaultdict
from typing import Dict, List, Optional
from queue import Queue
from threading import Thread

class LogDB:
    def __init__(self, db_path: str, index_interval: int = 1000, buffer_size: int = 10000, flush_interval: float = 0.1):
        """Initialize the log database with memory-mapped file and index

        Args:
            db_path (str): Path to the database file
            index_interval (int, optional): Number of entries between each index entry. Defaults to 1000.
            buffer_size (int, optional): Size of memory buffer before flush. Defaults to 10000.
            flush_interval (float, optional): Time in seconds between buffer flushes. Defaults to 0.1.
        """
        self.db_path = db_path
        self.index_interval = index_interval
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        
        self.index: Dict[str, List[int]] = defaultdict(list)  # list of file positions
        self.position_map: Dict[int, str] = {}  # line number to file position
        self.current_position = 0
        self.line_count = 0
        
        # Thread safety
        self.lock = threading.Lock()
        self.buffer_lock = threading.Lock()
        
        # Memory buffer
        self.buffer: List[Dict] = []
        self.buffer_queue: Queue = Queue()
        self.flush_thread: Optional[Thread] = None
        self.stop_flush = False

        # Create or open the database file
        if not os.path.exists(db_path):
            # Create with initial size of 8KB
            with open(db_path, 'wb') as f:
                f.write(b'\0' * 8192)  # Smaller initial size
        
        self.file = open(db_path, 'r+b')
        # Memory-map the file for efficient read/write operations
        self.mmap = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_WRITE)
        self._build_index()
        
        # Start background flush thread
        self.flush_thread = Thread(target=self._flush_buffer_periodic, daemon=True)
        self.flush_thread.start()

    def _build_index(self):
        """Build the initial index from existing file content"""
        with self.lock:
            self.mmap.seek(0)
            current_pos = 0
            
            while current_pos < self.mmap.size():
                self.mmap.seek(current_pos)
                line = self.mmap.readline()
                if not line or line.strip(b'\0') == b'':
                    break
                
                try:
                    entry = json.loads(line.strip(b'\0').decode('utf-8'))
                    if self.line_count == 0 or (self.line_count + 1) % self.index_interval == 0:
                        # Index every nth line
                        for key, value in entry.items():
                            if isinstance(value, (str, int, float)):
                                self.index[f"{key}:{value}"].append(current_pos)
                    
                    self.position_map[current_pos] = self.line_count
                    self.line_count += 1
                except json.JSONDecodeError:
                    pass  # ignore non-json lines
                
                current_pos = self.mmap.tell()
            
            self.current_position = current_pos

    def _flush_buffer_to_disk(self, entries: List[Dict]) -> bool:
        """Flush buffered entries to disk

        Args:
            entries (List[Dict]): List of log entries to flush

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.lock:
                for entry in entries:
                    log_line = json.dumps(entry) + '\n'
                    encoded_line = log_line.encode('utf-8')
                    
                    # Check if we need to resize the file
                    required_size = self.current_position + len(encoded_line)
                    current_size = self.mmap.size()
                    
                    if required_size > current_size:
                        # Double the size if needed
                        new_size = max(current_size * 2, required_size)
                        self.mmap.close()
                        self.file.truncate(new_size)
                        self.mmap = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_WRITE)

                    self.mmap.seek(self.current_position)
                    self.mmap.write(encoded_line)

                    # Index this entry if it's the first or an nth entry
                    if self.line_count == 0 or (self.line_count + 1) % self.index_interval == 0:
                        for key, value in entry.items():
                            if isinstance(value, (str, int, float)):
                                self.index[f"{key}:{value}"].append(self.current_position)

                    self.position_map[self.current_position] = self.line_count
                    self.current_position = self.mmap.tell()
                    self.line_count += 1
                
                self.mmap.flush()  # Force flush to disk
                return True
        except Exception:
            return False

    def _flush_buffer_periodic(self):
        """Background thread that periodically flushes the buffer"""
        while not self.stop_flush:
            time.sleep(self.flush_interval)
            
            with self.buffer_lock:
                if self.buffer:
                    entries = self.buffer[:]
                    self.buffer.clear()
                    self._flush_buffer_to_disk(entries)

    def write(self, log_entry: Dict) -> bool:
        """Write a log entry to the database

        Args:
            log_entry (Dict): Dictionary representing the log entry

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.buffer_lock:
                self.buffer.append(log_entry)
                
                # If buffer is full, flush to disk
                if len(self.buffer) >= self.buffer_size:
                    entries = self.buffer[:]
                    self.buffer.clear()
                    return self._flush_buffer_to_disk(entries)
                
                return True
        except Exception:
            return False

    def search(self, field: str, value: any) -> List[Dict]:
        """Search for log entries by field and value

        Args:
            field (str): Field to search
            value (any): Value to search for

        Returns:
            List[Dict]: List of matching log entries
        """
        results = []
        search_key = f"{field}:{value}"
        
        # First check memory buffer
        with self.buffer_lock:
            for entry in self.buffer:
                if str(entry.get(field)) == str(value):
                    results.append(entry)
        
        # Then check disk
        if search_key in self.index:
            with self.lock:
                for position in self.index[search_key]:
                    self.mmap.seek(position)
                    line = self.mmap.readline()
                    if line:
                        try:
                            entry = json.loads(line.strip(b'\0').decode('utf-8'))
                            if str(entry.get(field)) == str(value):
                                results.append(entry)
                        except json.JSONDecodeError:
                            continue
        
        return results

    def close(self):
        """Close the database and flush any remaining entries"""
        self.stop_flush = True
        if self.flush_thread:
            self.flush_thread.join()
        
        # Flush any remaining entries
        with self.buffer_lock:
            if self.buffer:
                self._flush_buffer_to_disk(self.buffer)
        
        self.mmap.flush()
        self.mmap.close()
        self.file.close()