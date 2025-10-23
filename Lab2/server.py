"""
HTTP File Server - Lab 2 (Concurrent)
A concurrent HTTP server using a thread pool, serving files from a specified directory.
Implements a thread-safe hit counter and IP-based rate limiting.
"""

import socket
import sys
import os
import time
import threading
import collections
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
from datetime import datetime
from rate_limiter import RateLimiter # Import new rate limiter class


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
        
        # --- Lab 2: Concurrency ---
        # Use a thread pool to handle multiple clients concurrently
        self.executor = ThreadPoolExecutor(max_workers=20)
        
        # --- Lab 2: Counter (2 points) ---
        # A dictionary to store hit counts for each file/directory path
        self.file_counts = collections.defaultdict(int)
        # A lock to make self.file_counts thread-safe
        self.count_lock = threading.Lock()
        
        # --- Lab 2: Rate Limiting (2 points) ---
        # 5 requests per second, per IP
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
        print(f"[INFO] Using ThreadPoolExecutor with max 20 workers")
        print(f"[INFO] Press Ctrl+C to stop the server\n")
        
        try:
            while True:
                client_socket, client_address = self.server_socket.accept()
                print(f"[CONNECTION] New connection from {client_address[0]}:{client_address[1]}")
                
                # --- Lab 2: Concurrency ---
                # Submit the request handling to the thread pool
                # This loop no longer blocks and can accept new connections
                self.executor.submit(self.handle_request, client_socket, client_address)
                
        except KeyboardInterrupt:
            print("\n[INFO] Server shutting down...")
        finally:
            self.executor.shutdown(wait=True)
            if self.server_socket:
                self.server_socket.close()
            print("[INFO] Server shutdown complete.")
    
    def handle_request(self, client_socket, client_address):
        """Handle a single HTTP request (runs in a worker thread)"""
        client_ip = client_address[0]
        
        # --- Lab 2: Rate Limiting (2 points) ---
        if not self.rate_limiter.allow(client_ip):
            print(f"[RATE_LIMIT] Denied request from {client_ip} (429 Too Many Requests)")
            self.send_error(client_socket, 429, "Too Many Requests")
            client_socket.close()
            return
            
        try:
            request_data = client_socket.recv(4096).decode('utf-8')
            if not request_data:
                return
            
            # --- Lab 2: Simulate Work Delay ---
            # Add ~1s delay to simulate work and test concurrency [cite: 1708]
            print(f"[THREAD] Handling request from {client_ip} (simulating 1s work)...")
            time.sleep(1)
            
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
            
            self.serve_path(client_socket, path)
            
        except Exception as e:
            print(f"[ERROR] Error handling request: {e}")
            try:
                self.send_error(client_socket, 500, "Internal Server Error")
            except:
                pass
        finally:
            client_socket.close()
            print(f"[CONNECTION] Closed connection from {client_ip}")
    
    def serve_path(self, client_socket, path):
        """Serve a file or directory listing"""
        
        original_path = path # Store original path for counter
        
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
            self.send_error(client_socket, 404, "Not Found")
            return
        
        if os.path.isdir(full_path):
            if not path.endswith('/'):
                path += '/'
            
            # --- Lab 2: Counter ---
            # Use original_path (e.g., "/Books/") as the key for the counter
            self.increment_counter(original_path if original_path.endswith('/') else original_path + '/')
            self.serve_directory_listing(client_socket, full_path, path)
            return
        
        if os.path.isfile(full_path):
            # --- Lab 2: Counter ---
            # Use original_path (e.g., "/Dog.png") as the key
            self.increment_counter(original_path)
            self.serve_file(client_socket, full_path)
    
    def increment_counter(self, path):
        """Thread-safely increment the hit counter for a given path"""
        # --- Lab 2: Counter (2 points) ---
        # Use the lock to prevent race conditions [cite: 1717]
        with self.count_lock:
            self.file_counts[path] += 1
            print(f"[COUNTER] Path {path} now has {self.file_counts[path]} hits.")

    def get_count(self, path):
        """Thread-safely get the hit counter for a given path"""
        with self.count_lock:
            return self.file_counts.get(path, 0)

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
            
            # --- Lab 2: Counter ---
            # Add "Hits" column to table header
            html += '<table>\n<thead><tr><th>Name</th><th class="hits">Hits</th><th class="size">Size</th></tr></thead>\n<tbody>\n'
            
            # Add directories
            for d in dirs:
                # Get count for the directory path (e.g., /Books/Subdir/)
                dir_full_path = url_path + d + '/'
                count = self.get_count(dir_full_path)
                html += f'<tr><td><a href="{d}/"><span class="icon">📁</span>{d}/</a></td><td class="hits">{count}</td><td class="size">-</td></tr>\n'
            
            # Add files
            for f in files:
                size = os.path.getsize(os.path.join(dir_path, f))
                size_str = self.format_size(size)
                
                # Get count for the file path (e.g., /Books/file.pdf)
                file_full_path = url_path + f
                count = self.get_count(file_full_path)
                
                icon = "📄"
                if f.endswith('.pdf'): icon = "📚"
                if f.endswith(('.png', '.jpg', '.jpeg', '.gif')): icon = "🖼️"
                if f.endswith('.html'): icon = "🌐"
                
                html += f'<tr><td><a href="{f}"><span class="icon">{icon}</span>{f}</a></td><td class="hits">{count}</td><td class="size">{size_str}</td></tr>\n'
            
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