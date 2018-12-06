import os
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, Response
from flask_socketio import SocketIO, emit, join_room
from datetime import datetime
from core.lib.jobs.monitor import ProgressObserver, SUBJOB_UPDATED, JOB_ENDED
from core.modules.statistics.StatsCenter import StatEventListener
import re
from fileinput import FileInput

__author__ = 'Federico Schmidt'


class WebServer(StatEventListener, ProgressObserver):
    app = Flask(__name__)
    socketio = SocketIO(app)

    def __init__(self, stats_center, scheduler, system_config, forecast_manager=None, max_logs=300):
        self.scheduler = scheduler
        self.stats = stats_center
        self.system_config = system_config
        self.forecast_manager = forecast_manager
        self.max_log_count = max_logs
        # Avoid the auto-reload feature in Flask.
        self.app.config.update(DEBUG=False)

        # Configure end points.
        self.app.add_url_rule('/', 'index', self.index)
        self.app.add_url_rule('/api/config', 'config', self.get_config, methods=['GET'])
        self.app.add_url_rule('/api/config/reload', 'reload_config', self.reload_config, methods=['GET'])
        self.app.add_url_rule('/api/forecasts', 'forecasts', self.get_forecasts, methods=['GET'])
        self.app.add_url_rule('/api/forecasts/reload/<file_name>', 'forecasts_reload', self.reload_forecast, methods=['GET'])
        self.app.add_url_rule('/api/forecasts/add_date/<file_name>/<new_date>', 'add_date', self.add_date, methods=['GET'])
        self.app.add_url_rule('/api/weather_data', 'weather_data', self.get_weather_data, methods=['GET'])
        self.app.add_url_rule('/api/job/run_now/<job_id>', 'run_job', self.run_job_now, methods=['GET'])
        self.app.add_url_rule('/api/job/cancel/<job_id>', 'cancel_job', self.cancel_job, methods=['GET'])
        self.app.add_url_rule('/<path:path>', 'index', self.index)

        # Configure socket-io end points.
        self.socketio.on_event('connected', self.connected, namespace='/observers')
        self.socketio.on_event('get_tasks', self.get_tasks, namespace='/observers')
        self.socketio.on_event('get_job_details', self.get_job_details, namespace='/observers')

        self.system_log = []

        # Add this class to the event listeners of the StatsCenter class.
        stats_center.event_listeners.append(self)

    def index(self, path=None):
        return self.app.send_static_file('index.html')

    def connected(self):
        """
        Emits only to the caller (via web sockets) the recen system logs.
        """
        emit('logs', self.system_log)

    def get_tasks(self):
        """
        Emits only to the caller (via web sockets) the pending and finished job queues.
        """
        emit('tasks', {
            'job_queue': self.stats.tasks,
            'finished_tasks': self.stats.finished_tasks
        })
        emit('active_tasks', self.stats.running_tasks)

    def get_job_details(self, job_id):
        """
        Retrieves the details of a job id, looking it up in every job queue in the system (pending, active, finished).
        Automatically joins the caller to the job room if an active job details are requested.
        :param job_id:
        """
        if job_id in self.stats.running_tasks:
            # Join this client to the job_id room.
            join_room(str(job_id))
            emit('job_details', self.stats.running_tasks[job_id])
        elif job_id in self.stats.tasks:
            emit('job_details', self.stats.tasks[job_id])
        elif job_id in self.stats.finished_tasks:
            emit('job_details', self.stats.finished_tasks[job_id])
        else:
            emit('job_details', None)

    def get_config(self):
        return Response(response=self.system_config.public_view(),
                        status=200,
                        mimetype="application/json")

    def reload_config(self):
        """
        Created a job that will acquire a system lock and try to reload the system configuration.
        If that job fails, the system config is not changed.
        """
        job = self.scheduler.add_job(self.system_config.reload, name='Reload configuration')
        return Response(response=job.id,
                        status=200,
                        mimetype="text/plain")

    def get_forecasts(self):
        """
        Returns the public view (stripped version of the Python object) of every forecast in every forecast
        specification file in the system.
        """
        forecasts = []
        for forecast_list in list(self.system_config.forecasts.values()):
            forecasts.extend([f.public_view(self.system_config.forecasts_path) for f in forecast_list])
        return Response(response='[%s]' % ','.join(forecasts),
                        status=200,
                        mimetype="application/json")

    def reload_forecast(self, file_name=None):
        """
        Creates a job that reloads a given forecast file.
        :param file_name:
        """
        if not file_name:
            return Response(status=404)

        if not self.forecast_manager:
            return Response(status=500)

        forecast_file = os.path.join(self.system_config.forecasts_path, file_name)

        if forecast_file not in self.system_config.forecasts:
            return Response(status=404)

        job = self.scheduler.add_job(self.system_config.forecasts_loader.reload_file,
                                     args=[forecast_file, self.scheduler, self.forecast_manager],
                                     name='Reload forecast file "%s"' % file_name)

        return Response(response=job.id,
                        status=200,
                        mimetype="text/plain")

    def add_date(self, file_name=None, new_date=None):
        """
        Creates a job that reloads a given forecast file.
        :param file_name:
        :param new_date:
        """
        if not file_name:
            return Response(status=404)

        if not self.forecast_manager:
            return Response(status=500)

        forecast_file = os.path.join(self.system_config.forecasts_path, file_name)

        if forecast_file not in self.system_config.forecasts:
            return Response(status=404)

        if new_date:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', new_date) and len(new_date) == 10:
                with FileInput(files=forecast_file, inplace=True) as input_file:
                    for line in input_file:
                        if re.match(r"^forecast_date:\s\['.*'\]$", line) and new_date not in line:
                            line = re.sub(r"^(forecast_date:\s\[.*)(\])$", r"\1, '{d}'\2".format(d=new_date), line)
                        print(line, end='')

        job = self.scheduler.add_job(self.system_config.forecasts_loader.reload_file,
                                     args=[forecast_file, self.scheduler, self.forecast_manager],
                                     name='Reload forecast file "%s"' % file_name)

        return Response(response=job.id,
                        status=200,
                        mimetype="text/plain")

    def get_weather_data(self):
        """
        Retrieve weather max dates for each station.
        :return: The JSON encoded dictionary found in the weather updater module.
        """
        if self.forecast_manager:
            return Response(response=self.forecast_manager.weather_updater.wth_max_date.to_json(),
                            status=200,
                            mimetype="application/json")
        else:
            return Response(status=500)

    def run_job_now(self, job_id=None):
        """
        Force a job to be ran as soon as possible (when a lock can be acquired).
        :param job_id:
        """
        if not job_id:
            return Response(status=404)

        if job_id not in self.stats.tasks:
            # Avoid trying to run an already running task or a finished one!
            return Response(status=403)

        job = self.scheduler.get_job(job_id)

        if not job:
            return Response(status=404)

        # If a job is periodic (has an interval trigger) it should be triggered by modifying the trigger it already has.
        # Otherwise, it can be rescheduled to be ran now.
        if isinstance(job.trigger, IntervalTrigger):
            self.scheduler.modify_job(job_id, next_run_time=datetime.now())
        else:
            job.reschedule(trigger='date', run_date=datetime.now())

        return Response(response=job.id,
                        status=200,
                        mimetype="text/plain")

    def cancel_job(self, job_id=None):
        """
        Cancels a pending job. If te job is running or has finished, returns an error.
        WARN: this method doesn't acquire a lock, so there's a  small (in milliseconds) time window in which the task
              may be triggered and we try to cancel it, but APScheduler won't remove a running task, so there's no
              possible harm.
        :param job_id:
        :return:
        """
        if not job_id:
            return Response(status=404)

        if job_id not in self.stats.tasks:
            # Avoid trying to cancel a running task or a finished one!
            return Response(status=403)

        job = self.scheduler.get_job(job_id)

        if not job:
            return Response(status=500)

        job.remove()

        return Response(response=job.id,
                        status=200,
                        mimetype="text/plain")

    def flush_logs(self, content):
        """
        Emits to every client connected to the webserver the new log contents.
        :param content:
        """
        self.system_log.extend(content)
        self.socketio.emit('logs', content, namespace='/observers')
        if len(self.system_log) > self.max_log_count:
            self.system_log = self.system_log[-self.max_log_count:]

    def stat_event(self, event):
        """
        Emits to every client (observer) connected to the web server the job queues.
        Note: this method could be improved by only emitting diffs, but this web server shouldn't have too many clients
              (< 10), so it's not worth the effort right now.
        :param event:
        """
        self.socketio.emit('tasks', {
            'job_queue': self.stats.tasks,
            'finished_tasks': self.stats.finished_tasks
        }, namespace='/observers')

    def progress_changed(self, event):
        """
        Emits progress to the corresponding observers: when a subjob is updated it only emits to clients registered to
        it's parent job's progress, otherwise, emits to every client to keep them updated about new jobs and subjobs.
        """
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
        try:
            self.socketio.run(self.app, host='0.0.0.0')
        except:
            self.socketio.run(self.app, host='0.0.0.0', port=5001)
