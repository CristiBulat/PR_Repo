"""
HTTP File Server - Lab 1
A simple HTTP server that serves HTML, PNG, and PDF files from a specified directory.
Supports directory listing for nested directories.
"""

import socket
import sys
import os
from urllib.parse import unquote
from datetime import datetime


class HTTPServer:
    """Simple HTTP/1.1 File Server"""
    
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
        
        if not os.path.exists(self.directory):
            raise ValueError(f"Directory '{self.directory}' does not exist")
        
        print(f"[INFO] Server initialized")
        print(f"[INFO] Serving directory: {self.directory}")
    
    def start(self):
        """Start the HTTP server and listen for connections"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        print(f"[INFO] Server listening on http://{self.host}:{self.port}")
        print(f"[INFO] Press Ctrl+C to stop the server\n")
        
        try:
            while True:
                client_socket, client_address = self.server_socket.accept()
                print(f"[CONNECTION] New connection from {client_address[0]}:{client_address[1]}")
                # Lab 1 is single-threaded
                self.handle_request(client_socket, client_address)
        except KeyboardInterrupt:
            print("\n[INFO] Server shutting down...")
        finally:
            if self.server_socket:
                self.server_socket.close()
    
    def handle_request(self, client_socket, client_address):
        """Handle a single HTTP request"""
        try:
            request_data = client_socket.recv(4096).decode('utf-8')
            if not request_data:
                return
            
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
    
    def serve_path(self, client_socket, path):
        """Serve a file or directory listing"""
        
        # Default to index.html for root path
        if path == '/':
            path = '/index.html'
        
        # Remove leading slash and resolve path
        safe_path = path.lstrip('/')
        
        # Prevent path traversal
        if '..' in safe_path:
            self.send_error(client_socket, 403, "Forbidden")
            return

        full_path = os.path.normpath(os.path.join(self.directory, safe_path))
        
        # Security check: ensure the resolved path is still within the serve directory
        if not full_path.startswith(self.directory):
            self.send_error(client_socket, 403, "Forbidden")
            return
        
        if not os.path.exists(full_path):
            self.send_error(client_socket, 404, "Not Found")
            return
        
        if os.path.isdir(full_path):
            # If a directory is requested, add a trailing slash for consistency
            if not path.endswith('/'):
                path += '/'
            self.serve_directory_listing(client_socket, full_path, path)
            return
        
        if os.path.isfile(full_path):
            self.serve_file(client_socket, full_path)
    
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
        """Generate and serve a directory listing page with a modified visual interface."""
        try:
            entries = os.listdir(dir_path)
            entries.sort()
            
            dirs = [e for e in entries if os.path.isdir(os.path.join(dir_path, e))]
            files = [e for e in entries if os.path.isfile(os.path.join(dir_path, e))]
            
            # Ensure url_path is correctly formatted (e.g., /Books/ or /)
            if not url_path.startswith('/'):
                url_path = '/' + url_path
            if not url_path.endswith('/'):
                url_path += '/'
            
            # --- MODIFIED VISUAL INTERFACE (Glassmorphism) ---
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
            background: #111827; /* Dark background */
            background-image: linear-gradient(135deg, #111827 0%, #37306B 100%);
            background-attachment: fixed;
            color: #F9FAFB;
        }}
        .container {{
            max-width: 900px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.05); /* Glass effect */
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
            color: #D1D5DB; /* Lighter text for headers */
            font-size: 14px;
            font-weight: 500;
        }}
        tr:hover {{
            background: rgba(255, 255, 255, 0.03);
        }}
        a {{
            text-decoration: none;
            color: #A5B4FC; /* Light purple/blue */
            font-weight: 500;
            transition: color 0.2s ease;
        }}
        a:hover {{
            color: #C7D2FE;
            text-decoration: underline;
        }}
        .icon {{
            display: inline-block;
            width: 24px;
            text-align: center;
            margin-right: 8px;
        }}
        .parent-link {{
            font-weight: 600;
            color: #E5E7EB;
            margin-bottom: 16px;
            display: inline-block;
        }}
        .size {{
            color: #9CA3AF; /* Muted text for size */
            font-size: 14px;
            text-align: right;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Index of {url_path}</h1>
"""
            
            # Add parent directory link
            if url_path != '/':
                parent_path = '/'.join(url_path.rstrip('/').split('/')[:-1]) + '/'
                if parent_path == '//': parent_path = '/'
                html += f'<a href="{parent_path}" class="parent-link"><span class="icon">⬆️</span> Parent Directory</a>\n'
            
            html += '<table>\n<thead><tr><th>Name</th><th style="text-align: right;">Size</th></tr></thead>\n<tbody>\n'
            
            # Add directories
            for d in dirs:
                html += f'<tr><td><a href="{d}/"><span class="icon">📁</span>{d}/</a></td><td class="size">-</td></tr>\n'
            
            # Add files
            for f in files:
                size = os.path.getsize(os.path.join(dir_path, f))
                size_str = self.format_size(size)
                
                # Determine file icon
                icon = "📄"
                if f.endswith('.pdf'): icon = "📚"
                if f.endswith(('.png', '.jpg', '.jpeg', '.gif')): icon = "🖼️"
                if f.endswith('.html'): icon = "🌐"
                
                html += f'<tr><td><a href="{f}"><span class="icon">{icon}</span>{f}</a></td><td class="size">{size_str}</td></tr>\n'
            
            html += """        </tbody>
    </table>
    </div>
</body>
</html>"""
            # --- END OF MODIFIED INTERFACE ---
            
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
        response += f"Server: Python-HTTP-Server/1.0-Modified\r\n"
        response += f"Date: {datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n"
        response += "Connection: close\r\n"
        response += "\r\n"
        
        client_socket.sendall(response.encode('utf-8') + body)
    
    def send_error(self, client_socket, status_code, status_text):
        """Send HTTP error response to client with new styling"""
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
            color: #F87171; /* Red color for error */
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
        self.send_response(client_socket, status_code, status_text, "text/html", body)
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