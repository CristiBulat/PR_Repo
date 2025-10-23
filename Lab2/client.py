"""
HTTP Client - Lab 2
A simple HTTP client that can download files from an HTTP server.
Supports HTML (prints to console), PNG and PDF (saves to directory).
"""

import socket
import sys
import os
from urllib.parse import urlparse


class HTTPClient:
    """Simple HTTP/1.1 Client"""
    
    def __init__(self, host, port, path, save_dir):
        """
        Initialize the HTTP client
        
        Args:
            host: Server hostname or IP address
            port: Server port number
            path: Path to request from the server
            save_dir: Directory to save downloaded files
        """
        self.host = host
        self.port = int(port)
        self.path = path if path.startswith('/') else '/' + path
        self.save_dir = save_dir
        
        # Create save directory if it doesn't exist
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)
            print(f"[INFO] Created directory: {self.save_dir}")
    
    def request(self):
        """Send HTTP GET request and handle response"""
        try:
            # Create socket connection
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.host, self.port))
            print(f"[INFO] Connected to {self.host}:{self.port}")
            
            # Construct HTTP GET request
            request = f"GET {self.path} HTTP/1.1\r\n"
            request += f"Host: {self.host}:{self.port}\r\n"
            request += "User-Agent: Python-HTTP-Client/1.0\r\n"
            request += "Connection: close\r\n"
            request += "\r\n"
            
            # Send request
            client_socket.sendall(request.encode('utf-8'))
            print(f"[REQUEST] GET {self.path}")
            
            # Receive response
            response_data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                response_data += chunk
            
            client_socket.close()
            
            # Parse response
            self.parse_response(response_data)
            
        except ConnectionRefusedError:
            print(f"[ERROR] Connection refused. Is the server running on {self.host}:{self.port}?")
            sys.exit(1)
        except socket.gaierror:
            print(f"[ERROR] Could not resolve hostname: {self.host}")
            sys.exit(1)
        except Exception as e:
            print(f"[ERROR] Request failed: {e}")
            sys.exit(1)
    
    def parse_response(self, response_data):
        """
        Parse HTTP response and handle based on content type
        
        Args:
            response_data: Raw HTTP response as bytes
        """
        try:
            # Split headers and body
            header_end = response_data.find(b"\r\n\r\n")
            if header_end == -1:
                print("[ERROR] Invalid HTTP response")
                return
            
            headers = response_data[:header_end].decode('utf-8')
            body = response_data[header_end + 4:]
            
            # Parse status line
            lines = headers.split('\r\n')
            status_line = lines[0]
            parts = status_line.split(' ', 2)
            
            if len(parts) < 3:
                print("[ERROR] Invalid status line")
                return
            
            status_code = int(parts[1])
            status_text = parts[2]
            
            print(f"[RESPONSE] {status_code} {status_text}")
            
            # Check status code
            if status_code != 200:
                print(f"[ERROR] Server returned error: {status_code} {status_text}")
                if body:
                    print("\nResponse body:")
                    print(body.decode('utf-8', errors='ignore'))
                return
            
            # Parse headers
            content_type = None
            content_length = None
            
            for line in lines[1:]:
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip().lower()
                    value = value.strip()
                    
                    if key == 'content-type':
                        content_type = value
                    elif key == 'content-length':
                        content_length = int(value)
            
            print(f"[INFO] Content-Type: {content_type}")
            print(f"[INFO] Content-Length: {content_length or len(body)} bytes")
            
            # Handle based on content type
            if content_type:
                if 'text/html' in content_type:
                    self.handle_html(body)
                elif 'image/png' in content_type:
                    self.handle_binary_file(body, 'png')
                elif 'application/pdf' in content_type:
                    self.handle_binary_file(body, 'pdf')
                else:
                    print(f"[WARNING] Unsupported content type: {content_type}")
                    # Try to save with a generic name
                    self.handle_binary_file(body, 'dat')
            else:
                print("[WARNING] No content type specified, printing as text.")
                self.handle_html(body) # Attempt to print
        
        except Exception as e:
            print(f"[ERROR] Error parsing response: {e}")
    
    def handle_html(self, body):
        """
        Handle HTML response by printing to console
        
        Args:
            body: Response body as bytes
        """
        try:
            html_content = body.decode('utf-8')
            print("\n" + "=" * 80)
            print("HTML CONTENT:")
            print("=" * 80)
            print(html_content)
            print("=" * 80)
        except Exception as e:
            print(f"[ERROR] Error decoding HTML: {e}")
    
    def handle_binary_file(self, body, extension):
        """
        Handle binary file response by saving to disk
        
        Args:
            body: Response body as bytes
            extension: File extension (png, pdf)
        """
        try:
            # Extract filename from path
            filename = os.path.basename(self.path)
            
            # If path is a directory or doesn't have extension, generate filename
            if not filename or '.' not in filename:
                # Use a default name if path is just "/" or "/Books/"
                filename = f"downloaded_file.{extension}"
            
            # Ensure extension matches content type
            if not filename.endswith(f'.{extension}'):
                 # Handle cases where extension is missing or wrong
                 base_name = os.path.splitext(filename)[0]
                 filename = f"{base_name}.{extension}"
            
            # Save file
            filepath = os.path.join(self.save_dir, filename)
            
            with open(filepath, 'wb') as f:
                f.write(body)
            
            print(f"\n[SUCCESS] File saved to: {filepath}")
            print(f"[INFO] File size: {len(body)} bytes")
            
        except Exception as e:
            print(f"[ERROR] Error saving file: {e}")


def main():
    """Main entry point"""
    if len(sys.argv) < 5:
        print("Usage: python client.py <server_host> <server_port> <url_path> <save_directory>")
        print("\nExamples:")
        print("  python client.py localhost 8080 / ./downloads")
        print("  python client.py localhost 8080 /Books/SomeBook.pdf ./downloads")
        sys.exit(1)
    
    host = sys.argv[1]
    port = sys.argv[2]
    path = sys.argv[3]
    save_dir = sys.argv[4]
    
    try:
        client = HTTPClient(host, port, path, save_dir)
        client.request()
    except Exception as e:
        print(f"[ERROR] Client failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()