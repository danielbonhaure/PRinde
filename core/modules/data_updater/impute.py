from core.lib.jobs.base import BaseJob
import re
from datetime import datetime
import shlex
import subprocess
import select
import sys
import os
import logging
from core.lib.jobs.monitor import NullMonitor, ProgressMonitor

__author__ = 'Federico Schmidt'


class RunImputation(BaseJob):

    def __init__(self, system_config, parent_task_monitor=None, max_events=10000):
        super(RunImputation, self).__init__(name='Impute missing data and calculate radiation',
                                            progress_monitor=ProgressMonitor())
        self.max_events = max_events
        self.station = re.compile('Station: (\d+)')
        self.system_config = system_config
        if not parent_task_monitor:
            parent_task_monitor = NullMonitor()
        self.parent_monitor = parent_task_monitor

    def run(self, weather_stations, verbose=True):
        logging.info('Running imputation job for stations: %s.' % weather_stations)

        self.progress_monitor.end_value = len(weather_stations)

        start_time = datetime.now()
        ret_val = self.__run__(weather_stations, verbose)
        end_time = datetime.now()

        logging.getLogger().info('Finished running imputation job for stations: %s. Retval = %s. Time: %s.' %
                                 (weather_stations, ret_val, end_time - start_time))

        return ret_val

    def __run__(self, weather_stations, verbose):
        stations = ','.join({str(id) for id in weather_stations})

        self.parent_monitor.add_subjob(self.progress_monitor, job_name=self.name)

        wth_db_config = self.system_config['database_config']['weather_db']
        module_path = os.path.dirname(os.path.realpath(__file__))

        command = 'Rscript "%s" ' % (os.path.join(module_path, 'impute_script', 'Main.R'))
        command += '--stations "%s" --parallelism %d ' % (stations, self.system_config['max_parallelism'])
        command += '--host "%s" --database "%s" --port %d --user "%s" ' % (wth_db_config['host'],
                                                                           wth_db_config['db_name'],
                                                                           wth_db_config['port'],
                                                                           wth_db_config['user'])
        command += '--password "%s" ' % wth_db_config['password']

        command = shlex.split(command)

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid)

        stdout_lines = []

        epoll = select.epoll()
        epoll.register(p.stdout.fileno(), select.EPOLLOUT | select.EPOLLIN)

        i = 0
        last_processed_station = None
        n_processed_stations = 0

        try:
            while i < self.max_events:
                i += 1
                events = epoll.poll(timeout=1)
                for fileno, event in events:

                    if event & select.EPOLLIN:
                        if fileno == p.stdout.fileno():
                            read = p.stdout.readline()
                            stdout_lines.append(read)

                            if verbose:
                                sys.stdout.write('# Impute job: ' + read)

                            station_id = self.station.search(read)

                            if station_id:
                                station_id = station_id.groups()[0]

                                if not last_processed_station:
                                    last_processed_station = station_id
                                else:
                                    if station_id != last_processed_station:
                                        last_processed_station = station_id
                                        n_processed_stations += 1
                                        self.progress_monitor.update_progress(new_value=n_processed_stations)

                            if 'Success' in read:
                                self.progress_monitor.update_progress(new_value=self.progress_monitor.end_value)
                                return 0

                    elif event & select.EPOLLHUP:
                        if fileno == p.stdout.fileno():
                            raise RuntimeError("Program ended.")

        except RuntimeError, err:
            print(err.message)
        finally:
            epoll.unregister(p.stdout.fileno())

        return p.poll()
