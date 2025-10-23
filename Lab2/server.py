"""
HTTP File Server - Lab 2 (Concurrent)

A concurrent HTTP server using a thread pool, serving files from a specified directory.
Implements a thread-safe hit counter and IP-based rate limiting.

This version is structured to support different modes for testing:
- "single":     Single-threaded, blocking (Lab 1 behavior)
- "multi":      Multi-threaded via ThreadPool (Lab 2 concurrency test)
- "race":       Multi-threaded with a naive, broken counter (Lab 2 race-condition test)
- "threadsafe": Multi-threaded with a lock-protected counter (Lab 2 fix)
- "ratelimit":  Multi-threaded, thread-safe counter, and IP rate limiting (Lab 2 final)
"""

import socket
import sys
import os
import time  # Make sure time is imported
import threading
import collections
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
from datetime import datetime
from rate_limiter import RateLimiter  # Import your rate limiter class

# === CONFIGURE SERVER MODE HERE ===
# Change this to: "single", "multi", "race", "threadsafe", "ratelimit"
SERVER_MODE = "ratelimit"
# ==================================

class HTTPServer:
    """Concurrent HTTP/1.1 File Server"""
    
    MIME_TYPES = {
        '.html': 'text/html',
        '.htm': 'text/html',
        '.png': 'image/png',
        '.pdf': 'application/pdf',
        '.css': 'text/css',
        '.js': 'application/javascript',
    }
    
    def __init__(self, host='0.0.0.0', port=8080, directory='.'):
        self.host = host
        self.port = port
        self.directory = os.path.abspath(directory)
        self.server_socket = None
        
        # ThreadPoolExecutor handles creating/managing threads.
        # This is used for all modes except "single"
        self.executor = ThreadPoolExecutor(max_workers=20)
        
        # Shared resources for counter modes
        self.file_counts = collections.defaultdict(int)
        self.count_lock = threading.Lock()
        
        # Your rate limiter, used only in "ratelimit" mode
        self.rate_limiter = RateLimiter(limit=5, per_second=1)

        if not os.path.exists(self.directory):
            raise ValueError(f"Directory '{self.directory}' does not exist")
        
        print(f"[INFO] Server initialized")
        print(f"[INFO] Serving directory: {self.directory}")
    
    def start(self):
        """Start the HTTP server and listen for connections"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        
        print(f"[INFO] Server listening on http://{self.host}:{self.port}")
        print(f"[INFO] *** SERVER RUNNING IN '{SERVER_MODE}' MODE ***")
        if SERVER_MODE != "single":
            print(f"[INFO] Using ThreadPoolExecutor with max 20 workers")
        print(f"[INFO] Press Ctrl+C to stop the server\n")
        
        try:
            while True:
                # Accept connection in the main thread
                client_socket, client_address = self.server_socket.accept()
                print(f"[CONNECTION] New connection from {client_address[0]}:{client_address[1]}")
                
                # --- Mode-switching logic ---
                if SERVER_MODE == "single":
                    # For "single" mode, handle the request directly in the
                    # main thread. This is blocking, as required for the test.
                    self.handle_request(client_socket, client_address)
                else:
                    # For all other modes ("multi", "race", "threadsafe", "ratelimit"),
                    # submit the request to the thread pool to be handled concurrently.
                    self.executor.submit(self.handle_request, client_socket, client_address)
                
        except KeyboardInterrupt:
            print("\n[INFO] Server shutting down...")
        finally:
            self.executor.shutdown(wait=True)
            if self.server_socket:
                self.server_socket.close()
            print("[INFO] Server shutdown complete.")
    
    def handle_request(self, client_socket, client_address):
        """
        Handle a single HTTP request.
        This runs in the main thread for "single" mode
        or in a worker thread for all other modes.
        """
        client_ip = client_address[0]
        
        # --- Rate Limit Check ---
        # Only apply rate limiting if in the correct mode
        if SERVER_MODE == "ratelimit":
            if not self.rate_limiter.allow(client_ip):
                print(f"[RATE_LIMIT] Denied request from {client_ip} (429 Too Many Requests)")
                self.send_error(client_socket, 429, "Too Many Requests")
                client_socket.close()
                return
            
        try:
            # 1. Read the request data
            request_data = client_socket.recv(4096).decode('utf-8')
            if not request_data:
                return
            
            # --- Lab 2 Concurrency Test Delay ---
            # Add 1s delay to simulate work, as required by the lab pdf.
            # This makes the "single" vs "multi" test possible.
            print(f"[THREAD] Handling request from {client_ip} (simulating 1s work)...")
            time.sleep(1)
            
            # 3. Process the request
            lines = request_data.split('\r\n')
            request_line = lines[0]
            print(f"[REQUEST] {request_line}")
            
            parts = request_line.split()
            if len(parts) < 2:
                self.send_error(client_socket, 400, "Bad Request")
                return
            
            method = parts[0]
            path = unquote(parts[1])
            
            if method != 'GET':
                self.send_error(client_socket, 405, "Method Not Allowed")
                return
            
            # 4. Serve the path (which will send the response)
            self.serve_path(client_socket, path)
            
        except Exception as e:
            print(f"[ERROR] Error handling request: {e}")
            try:
                self.send_error(client_socket, 500, "Internal Server Error")
            except:
                pass
        finally:
            # This closes the socket for this specific request
            client_socket.close()
            print(f"[CONNECTION] Closed connection from {client_ip}")
    
    def serve_path(self, client_socket, path):
        """Serve a file or directory listing"""
        
        original_path = path  # Store original path for counter
        
        # If root is requested, your collection/index.html will be served
        if path == '/':
            path = '/index.html'
        
        safe_path = path.lstrip('/')
        
        if '..' in safe_path:
            self.send_error(client_socket, 403, "Forbidden")
            return

        full_path = os.path.normpath(os.path.join(self.directory, safe_path))
        
        if not full_path.startswith(self.directory):
            self.send_error(client_socket, 403, "Forbidden")
            return
        
        if not os.path.exists(full_path):
            # This logic handles if /index.html doesn't exist,
            # but / was requested, it serves the directory listing instead.
            if path == '/index.html' and original_path == '/':
                self.increment_counter(original_path) # Count the directory '/'
                self.serve_directory_listing(client_socket, self.directory, '/')
                return

            self.send_error(client_socket, 404, "Not Found")
            return
        
        if os.path.isdir(full_path):
            if not path.endswith('/'):
                path += '/'
            
            # Check for an index.html in the subdirectory (e.g., /Books/index.html)
            index_path = os.path.join(full_path, 'index.html')
            if os.path.exists(index_path):
                self.increment_counter(original_path if original_path.endswith('/') else original_path + '/')
                self.serve_file(client_socket, index_path)
            else:
                # No index.html, serve the directory listing
                self.increment_counter(original_path if original_path.endswith('/') else original_path + '/')
                self.serve_directory_listing(client_socket, full_path, path)
            return
        
        if os.path.isfile(full_path):
            # This handles the case where /index.html *was* found
            if path == '/index.html' and original_path == '/':
                self.increment_counter(original_path) # Count '/'
            else:
                self.increment_counter(original_path) # Count the file
            self.serve_file(client_socket, full_path)
    
    def increment_counter(self, path):
        """Thread-safely increment the hit counter for a given path"""
        
        if SERVER_MODE == "race":
            # --- Naive implementation ---
            # This code is intentionally broken to demonstrate a race condition.
            current_count = self.file_counts.get(path, 0)
            time.sleep(0.001) # Small delay to make a race condition more likely
            self.file_counts[path] = current_count + 1
            print(f"[COUNTER] Path {path} now has {self.file_counts[path]} hits (RACE).")

        elif SERVER_MODE == "threadsafe" or SERVER_MODE == "ratelimit":
            # --- Thread-Safe implementation ---
            # This code uses a lock to protect the shared dictionary
            with self.count_lock:
                current_count = self.file_counts.get(path, 0)
                time.sleep(0.001) # Simulate work
                self.file_counts[path] = current_count + 1
                print(f"[COUNTER] Path {path} now has {self.file_counts[path]} hits (SAFE).")
        
        # In "single" or "multi" modes, this function does nothing.

    def get_count(self, path):
        """Thread-safely get the hit counter for a given path"""
        count = 0
        if SERVER_MODE in ["race", "threadsafe", "ratelimit"]:
            # Reads must also be locked to prevent reading
            # while another thread is in the middle of writing.
            with self.count_lock:
                count = self.file_counts.get(path, 0)
        return count

    def serve_file(self, client_socket, file_path):
        """Serve a file to the client"""
        _, ext = os.path.splitext(file_path)
        content_type = self.MIME_TYPES.get(ext.lower(), 'application/octet-stream')
        
        try:
            with open(file_path, 'rb') as f:
                body = f.read()
            
            self.send_response(client_socket, 200, "OK", content_type, body)
            print(f"[RESPONSE] 200 OK - Served file: {os.path.basename(file_path)} ({len(body)} bytes)")
            
        except Exception as e:
            print(f"[ERROR] Error reading file: {e}")
            self.send_error(client_socket, 500, "Internal Server Error")
    
    def serve_directory_listing(self, client_socket, dir_path, url_path):
        """Generate and serve a directory listing page with hit counter."""
        try:
            entries = os.listdir(dir_path)
            entries.sort()
            
            dirs = [e for e in entries if os.path.isdir(os.path.join(dir_path, e))]
            files = [e for e in entries if os.path.isfile(os.path.join(dir_path, e))]
            
            if not url_path.startswith('/'):
                url_path = '/' + url_path
            if not url_path.endswith('/'):
                url_path += '/'
            
            # --- Use your beautiful HTML template ---
            html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Index of {url_path}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 40px;
            background: #111827;
            background-image: linear-gradient(135deg, #111827 0%, #37306B 100%);
            background-attachment: fixed;
            color: #F9FAFB;
        }}
        .container {{
            max-width: 900px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 32px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
            backdrop-filter: blur(10px);
        }}
        h1 {{
            font-size: 28px;
            color: #FFFFFF;
            border-bottom: 1px solid rgba(255, 255, 255, 0.2);
            padding-bottom: 16px;
            margin: 0 0 24px 0;
            word-wrap: break-word;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px 8px;
            text-align: left;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }}
        th {{
            color: #D1D5DB;
            font-size: 14px;
            font-weight: 500;
        }}
        tr:hover {{
            background: rgba(255, 255, 255, 0.03);
        }}
        a {{
            text-decoration: none;
            color: #A5B4FC;
            font-weight: 500;
            transition: color 0.2s ease;
        }}
        a:hover {{
            color: #C7D2FE;
            text-decoration: underline;
        }}
        .icon {{ display: inline-block; width: 24px; text-align: center; margin-right: 8px; }}
        .parent-link {{ font-weight: 600; color: #E5E7EB; margin-bottom: 16px; display: inline-block; }}
        .size, .hits {{
            color: #9CA3AF;
            font-size: 14px;
            text-align: right;
            padding-right: 15px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Index of {url_path}</h1>
"""
            
            if url_path != '/':
                parent_path = '/'.join(url_path.rstrip('/').split('/')[:-1]) + '/'
                if parent_path == '//': parent_path = '/'
                html += f'<a href="{parent_path}" class="parent-link"><span class="icon">⬆️</span> Parent Directory</a>\n'
            
            # --- Conditional Hits Column ---
            # Only show the "Hits" column if in a mode that supports it
            show_hits = SERVER_MODE in ["race", "threadsafe", "ratelimit"]
            
            html += '<table>\n<thead><tr><th>Name</th>'
            if show_hits:
                html += '<th class="hits">Hits</th>'
            html += '<th class="size">Size</th></tr></thead>\n<tbody>\n'
            
            # Add directories
            for d in dirs:
                dir_full_path = url_path + d + '/'
                html += f'<tr><td><a href="{d}/"><span class="icon">📁</span>{d}/</a></td>'
                if show_hits:
                    count = self.get_count(dir_full_path)
                    html += f'<td class="hits">{count}</td>'
                html += '<td class="size">-</td></tr>\n'
            
            # Add files
            for f in files:
                size = os.path.getsize(os.path.join(dir_path, f))
                size_str = self.format_size(size)
                file_full_path = url_path + f
                
                icon = "📄"
                if f.endswith('.pdf'): icon = "📚"
                if f.endswith(('.png', '.jpg', '.jpeg', '.gif')): icon = "🖼️"
                if f.endswith('.html'): icon = "🌐"
                
                html += f'<tr><td><a href="{f}"><span class="icon">{icon}</span>{f}</a></td>'
                if show_hits:
                    count = self.get_count(file_full_path)
                    html += f'<td class="hits">{count}</td>'
                html += f'<td class="size">{size_str}</td></tr>\n'
            
            html += """        </tbody>
    </table>
    </div>
</body>
</html>"""
            
            body = html.encode('utf-8')
            self.send_response(client_socket, 200, "OK", "text/html", body)
            print(f"[RESPONSE] 200 OK - Served directory listing: {url_path} ({len(dirs)} dirs, {len(files)} files)")
            
        except Exception as e:
            print(f"[ERROR] Error generating directory listing: {e}")
            self.send_error(client_socket, 500, "Internal Server Error")
    
    def format_size(self, size):
        """Format file size in human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:,.0f} {unit}" if unit == 'B' else f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
    
    def send_response(self, client_socket, status_code, status_text, content_type, body):
        """Send HTTP response to client"""
        response = f"HTTP/1.1 {status_code} {status_text}\r\n"
        response += f"Content-Type: {content_type}\r\n"
        response += f"Content-Length: {len(body)}\r\n"
        response += f"Server: Python-HTTP-Server/2.0-Concurrent\r\n"
        response += f"Date: {datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n"
        response += "Connection: close\r\n"
        response += "\r\n"
        
        client_socket.sendall(response.encode('utf-8') + body)
    
    def send_error(self, client_socket, status_code, status_text):
        """Send HTTP error response to client with styling"""
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{status_code} {status_text}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: #111827;
            color: #F9FAFB;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
        }}
        .error-container {{
            text-align: center;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 48px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
            backdrop-filter: blur(10px);
        }}
        h1 {{
            color: #F87171; /* Red for error */
            font-size: 60px;
            margin: 0;
        }}
        p {{
            color: #E5E7EB;
            font-size: 20px;
            margin: 16px 0 0 0;
        }}
    </style>
</head>
<body>
    <div class="error-container">
        <h1>{status_code}</h1>
        <p>{status_text}</p>
    </div>
</body>
</html>"""
        
        body = html.encode('utf-8')
        try:
            self.send_response(client_socket, status_code, status_text, "text/html", body)
        except Exception as e:
            print(f"[ERROR] Failed to send error response: {e}")
        print(f"[RESPONSE] {status_code} {status_text}")


def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python server.py <directory>")
        print("Example: python server.py ./collection")
        sys.exit(1)
    
    directory = sys.argv[1]
    
    try:
        server = HTTPServer(host='0.0.0.0', port=8080, directory=directory)
        server.start()
    except Exception as e:
        print(f"[ERROR] Failed to start server: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()