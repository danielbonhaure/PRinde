__author__ = 'Federico Schmidt'

import yaml
import os.path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading


class Configuration(FileSystemEventHandler):

    def __init__(self, root_path):

        # Create system configuration path relative to root.
        self.config_path = os.path.join(root_path, 'config', 'system.yaml')

        if not os.path.isfile(self.config_path):
            raise RuntimeError('System configuration file not found.')

        self.watch_thread = None

    def load(self):
        # Load system configuration from the YAML file and update this object's dictionary to add the keys found
        # in the config file, allowing us to access them using the dot notation (eg. "config.temp_folder" instead of
        # "config.get('temp_folder')", though this is also supported.
        system_config = yaml.safe_load(open(self.config_path))
        self.__dict__.update(system_config)

        # If the watch thread isn't already loaded, create and start it.
        if not self.watch_thread:
            # Create a Thread with "watch" function and start it.
            self.watch_thread = threading.Thread(target=self.watch)
            self.watch_thread.start()

    def get(self, key, default=None):
        """
        Provides a Python's dictionary alike way of accessing configuration properties, with default values in case
        of missingness.
        :param key: The key you're looking for in the configuration.
        :param default: The value that should be returned if the key is not found.
        :return: The value associated with the key provided, or the default value if not found.
        """
        return self.__dict__.get(key, default)

    def watch(self):
        """
        This function is a new thread, instantiates the watchdog.Observer thread and registers this object (self)
        as the Event Handler (see watchdog's documentation).
        :return: None
        """
        # We'll watch the system's config parent path.
        watch_path = os.path.dirname(self.config_path)

        observer = Observer()
        observer.schedule(self, watch_path, recursive=False)
        observer.start()
        observer.join()

    def on_modified(self, event):
        """
        Overrides the FileSystemEventHandler.on_modified function, will be triggered every time a file or directory
        changes inside the configuration folder.
        :param event: The object representing the event, see Watchdog's documentation.
        :return: None
        """
        if event.src_path == self.config_path:
            # Reload config.
            self.load()
        pass
