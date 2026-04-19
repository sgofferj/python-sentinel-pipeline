#!/usr/bin/env python3
"""
Specialized HTTP Server for Sentinel Viewer.
Supports HTTP Range Requests, which are required for COG (Cloud Optimized GeoTIFF) streaming.
Usage: python3 serve.py
"""

import http.server
import os
import re

class RangeRequestHandler(http.server.SimpleHTTPRequestHandler):
    """
    HTTP Handler that adds support for the 'Range' header.
    Needed by georaster-layer-for-leaflet to stream specific chunks of large TIFs.
    """
    def end_headers(self):
        # Enable CORS for all origins
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Range, Content-Type')
        self.send_header('Access-Control-Expose-Headers', 'Content-Range, Content-Length, Accept-Ranges')
        
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if 'Range' not in self.headers:
            return super().do_GET()

        range_header = self.headers['Range']
        match = re.match(r'bytes=(\d+)-(\d*)', range_header)
        if not match:
            return super().do_GET()

        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            return super().do_GET()

        file_size = os.path.getsize(path)
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else file_size - 1

        if start >= file_size:
            self.send_error(416, 'Requested Range Not Satisfiable')
            return

        self.send_response(206)
        self.send_header('Content-Type', self.guess_type(path))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
        self.send_header('Content-Length', str(end - start + 1))
        self.end_headers()

        with open(path, 'rb') as f:
            f.seek(start)
            self.wfile.write(f.read(end - start + 1))

if __name__ == "__main__":
    import sys
    port = 8080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    
    # Start the server from the current directory
    server_address = ('', port)
    httpd = http.server.HTTPServer(server_address, RangeRequestHandler)
    print(f"Sentinel Pipeline Server running at http://localhost:{port}/")
    print("Serving from:", os.getcwd())
    print("Supports Range Requests and CORS for COG streaming.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        sys.exit(0)
