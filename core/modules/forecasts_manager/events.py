from core.lib.utils.log import log_format_exception

__author__ = 'Federico Schmidt'
import signal
import sys
import logging
import traceback


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.getLogger().error("Uncaught exception. %s" %
                              log_format_exception(traceback.format_exception(exc_type, exc_value, exc_traceback)))


# sys.excepthook = handle_exception


def register_signals():
    signal.signal(signal.SIGINT, shutdown_system)
    signal.signal(signal.SIGTERM, shutdown_system)
    return


def shutdown_system():
    print("Shutting down.")
    return
