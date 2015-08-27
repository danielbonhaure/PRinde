from flask import Flask, request, Response
from flask.ext.socketio import SocketIO

from lib.jobs.monitor import ProgressObserver
from core.stats.StatsCenter import StatEventListener

__author__ = 'Federico Schmidt'


class WebServer(StatEventListener, ProgressObserver):
    app = Flask(__name__)
    socketio = SocketIO(app)

    def __init__(self, stats_center, scheduler, system_config, max_logs=300):
        self.scheduler = scheduler
        self.stats = stats_center
        self.system_config = system_config
        self.max_log_count = max_logs
        self.app.config.update(DEBUG=False)
        self.app.add_url_rule('/', 'index', self.index)
        self.app.add_url_rule('/api/config', 'config', self.get_config, methods=['GET'])
        self.app.add_url_rule('/api/forecasts', 'forecasts', self.get_forecasts, methods=['GET'])
        self.app.add_url_rule('/<path:path>', 'index', self.index)
        self.socketio._on_message('connected', self.connected, namespace='/observers')
        self.socketio._on_message('get_tasks', self.get_tasks, namespace='/observers')
        self.system_log = []

        # Add this class to the event listeners of the StatsCenter class.
        stats_center.event_listeners.append(self)

    def index(self, path=None):
        return self.app.send_static_file('index.html')

    def connected(self):
        request.namespace.emit('logs', self.system_log)

    def get_tasks(self):
        request.namespace.emit('tasks', {
            'job_queue': self.stats.tasks,
            'finished_tasks': self.stats.finished_tasks
        })
        request.namespace.emit('active_tasks', self.stats.running_tasks)

    def get_config(self):
        return Response(response=self.system_config.public_view(),
                        status=200,
                        mimetype="application/json")

    def get_forecasts(self):
        forecasts = [f.public_view() for f in self.system_config.forecasts]
        return Response(response='[%s]' % ','.join(forecasts),
                        status=200,
                        mimetype="application/json")

    def flush_logs(self, content):
        self.system_log.extend(content)
        self.socketio.emit('logs', content, namespace='/observers')
        if len(self.system_log) > self.max_log_count:
            self.system_log = self.system_log[-self.max_log_count:]

    def stat_event(self, event):
        self.socketio.emit('tasks', {
            'job_queue': self.stats.tasks,
            'finished_tasks': self.stats.finished_tasks
        }, namespace='/observers')

    def progress_changed(self, event):
        self.socketio.emit('active_tasks_event', event.serialize(), namespace='/observers')

    def start(self):
        self.socketio.run(self.app)
