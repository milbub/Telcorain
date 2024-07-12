"""Module containing the HTTP server related functions."""
from http.server import SimpleHTTPRequestHandler, HTTPServer
import os
import threading

from handlers import config_handler
from handlers.logging_handler import logger


class OutputsHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Custom HTTP request handler for serving files from the outputs directory."""
    outputs_dir = config_handler.read_option("directories", "outputs_web")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=OutputsHTTPRequestHandler.outputs_dir, **kwargs)

    def log_message(self, format: str, *args):
        """Overrides the log_message method to log the client's IP address and requested path."""
        if self.path == "/":
            logger.debug(
                "HTTP is serving directory listing to client: \"%s\" on port: %d.",
                self.client_address[0],
                self.client_address[1],
            )
        else:
            logger.debug(
                "HTTP is serving file: \"%s%s\" to client: \"%s\" on port: %d.",
                OutputsHTTPRequestHandler.outputs_dir,
                self.path,
                self.client_address[0],
                self.client_address[1],
            )

    def log_error(self, format, *args):
        """Override the log_error method to do nothing since the error is already logged in send_error."""
        return

    def send_error(self, code: int, message: str = None, explain: str = None):
        """Overrides the send_error method to log the error code and message together with client IP and file path."""
        logger.warning(
            "HTTP error %d during file request: \"%s%s\", client: \"%s\" on port: %d. %s.",
            code,
            OutputsHTTPRequestHandler.outputs_dir,
            self.path,
            self.client_address[0],
            self.client_address[1],
            message
        )
        super().send_error(code, message, explain)

def setup_http_server():
    """
    Sets up and runs the HTTP server for serving files from the outputs directory.
    Must be run in a separate thread.
    """
    is_enabled = config_handler.read_option("realtime", "enable_http_server")
    if is_enabled.lower() == "true":
        logger.info("Starting HTTP server...")

        if not os.path.exists(OutputsHTTPRequestHandler.outputs_dir):
            os.makedirs(OutputsHTTPRequestHandler.outputs_dir)
            logger.debug("Created %s directory for HTTP server files.", OutputsHTTPRequestHandler.outputs_dir)

        address = config_handler.read_option("realtime", "http_server_address")
        port = int(config_handler.read_option("realtime", "http_server_port"))
        if address == "0.0.0.0":
            address_t = ""
        else:
            address_t = address
        socket = (address_t, port)
        logger.info(f"HTTP server is running on {address}:{port}.")
        logger.debug(f"HTTP server is serving files from directory: {OutputsHTTPRequestHandler.outputs_dir}")

        httpd = HTTPServer(socket, OutputsHTTPRequestHandler)
        httpd.serve_forever()
    else:
        logger.info("HTTP server is disabled.")


def start_http_server_thread():
    """Starts the HTTP server in a separate thread."""
    http_server_thread = threading.Thread(target=setup_http_server, daemon=True, name="HTTPServer")
    http_server_thread.start()
    return http_server_thread
