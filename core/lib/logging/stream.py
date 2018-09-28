import threading
import io
__author__ = 'federico'


class WebStream:

    def __init__(self, web_server):
        self.web_server = web_server
        self.write_lock = threading.Lock()
        self.logs_array = []

    def write(self, s):
        with self.write_lock:
            self.logs_array.append(s)

    def flush(self):
        with self.write_lock:
            # Send to web socket..
            self.web_server.flush_logs(self.logs_array)
            # Truncate buffer.
            self.logs_array = []
