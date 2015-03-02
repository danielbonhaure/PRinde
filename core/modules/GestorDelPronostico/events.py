__author__ = 'Federico Schmidt'
import signal


def register_signals():
    signal.signal(signal.SIGINT, shutdown_system)
    signal.signal(signal.SIGTERM, shutdown_system)
    return


def shutdown_system():
    print("Shutting down.")
    return