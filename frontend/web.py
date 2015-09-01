import logging

from flask import Flask, request, Response
from flask.ext.socketio import SocketIO

from lib.jobs.monitor import ProgressObserver, SUBJOB_UPDATED, JOB_ENDED
from modules.statistics import StatEventListener

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
        self.socketio._on_message('get_job_details', self.get_job_details, namespace='/observers')
        self.system_log = []

        # Add this class to the event listeners of the StatsCenter class.
        stats_center.event_listeners.append(self)

    def index(self, path=None):
        return self.app.send_static_file('index.html')

    def connected(self):
        request.namespace.emit('logs', self.system_log)

    def get_tasks(self):
        logging.getLogger().debug('WS: Get tasks.')
        request.namespace.emit('tasks', {
            'job_queue': self.stats.tasks,
            'finished_tasks': self.stats.finished_tasks
        })
        request.namespace.emit('active_tasks', self.stats.running_tasks)

    def get_job_details(self, job_id):
        logging.getLogger().debug('WS: Get job details.')
        if job_id in self.stats.running_tasks:
            # Join this client to the job_id room.
            request.namespace.join_room(str(job_id))
            request.namespace.emit('job_details', self.stats.running_tasks[job_id])
        elif job_id in self.stats.tasks:
            request.namespace.emit('job_details', self.stats.tasks[job_id])
        elif job_id in self.stats.finished_tasks:
            request.namespace.emit('job_details', self.stats.finished_tasks[job_id])
        else:
            request.namespace.emit('job_details', None)

    def get_config(self):
        return Response(response=self.system_config.public_view(),
                        status=200,
                        mimetype="application/json")

    def get_forecasts(self):
        forecasts = []
        for forecast_list in self.system_config.forecasts.values():
            forecasts.extend([f.public_view(self.system_config.forecasts_path) for f in forecast_list])
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
        job_id = str(event.job['id'])

        if event.job['parent']:
            job_id = str(event.job['parent'])

        if event.event_type & JOB_ENDED:
            self.socketio.close_room(job_id)

        if SUBJOB_UPDATED & event.event_type:
            # Do not emit to everybody about subjobs' update events.
            self.socketio.emit('active_tasks_event', event.serialize(), namespace='/observers', room=job_id)
        else:
            self.socketio.emit('active_tasks_event', event.serialize(), namespace='/observers')

    def start(self):
        self.socketio.run(self.app)
