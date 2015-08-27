import re
from datetime import datetime
import shlex
import subprocess
import select
import sys
import os
import signal
import logging

__author__ = 'Federico Schmidt'


class RunpSIMS:

    def __init__(self, run_lock, max_events=10000):
        self.concurrency_lock = run_lock
        self.max_events = max_events
        self.selecting = re.compile('Selecting site:(\d+)')
        self.active = re.compile('Active:(\d+)')
        self.finished = re.compile('Finished successfully:(\d+)')
        self.submitted = re.compile('Submitted:(\d+)')
        self.submitting = re.compile('Submitting:(\d+)')
        self.stage_out = re.compile('Stage out:(\d+)')
        self.stage_in = re.compile('Stage in:(\d+)')
        self.failed = re.compile('Failed:(\d+)')

    def run(self, forecast, verbose=True):
        with self.concurrency_lock:
            logging.getLogger().info('Running pSIMS for forecast "%s" (%s).' %
                                           (forecast.name, forecast.forecast_date))
            start_time = datetime.now()
            ret_val = self.__run__(forecast.paths.run_script_path, forecast.name, forecast.simulation_count, verbose)
            end_time = datetime.now()

            logging.getLogger().info('Finished running pSIMS for forecast "%s" (%s). Retval = %d. Time: %s.' %
                                           (forecast.name, forecast.forecast_date, ret_val, end_time - start_time))

            return ret_val

    def __run__(self, sh_script, forecast_name, sim_count, verbose):
        command = shlex.split('sh "%s"' % sh_script)

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid)

        stdout_lines = []

        epoll = select.epoll()
        epoll.register(p.stdout.fileno(), select.EPOLLOUT | select.EPOLLIN)

        i = 0

        try:
            while i < self.max_events:
                i += 1
                events = epoll.poll(timeout=1)
                for fileno, event in events:

                    if event & select.EPOLLIN:
                        if fileno == p.stdout.fileno():
                            read = p.stdout.readline()
                            stdout_lines.append(read)
                            # if verbose:
                            #     sys.stdout.write('> ' + read)
                            if read.startswith('Progress'):
                                selecting = self.selecting.search(read)
                                active = self.active.search(read)
                                finished = self.finished.search(read)
                                submitted = self.submitted.search(read)
                                submitting = self.submitting.search(read)
                                stage_out = self.stage_out.search(read)
                                stage_in = self.stage_in.search(read)
                                failed = self.failed.search(read)
                                running = 0
                                completed = 0
                                total = 0

                                if active:
                                    running = int(active.groups()[0])
                                    total += running
                                if selecting:
                                    total += int(selecting.groups()[0])
                                if finished:
                                    completed = int(finished.groups()[0])
                                    total += completed
                                if submitted:
                                    total += int(submitted.groups()[0])
                                if submitting:
                                    total += int(submitting.groups()[0])
                                if stage_out:
                                    total += int(stage_out.groups()[0])
                                if stage_in:
                                    total += int(stage_in.groups()[0])

                                if total == 0:
                                    continue

                                if total != sim_count or failed:
                                    logging.getLogger().error("pSIMS total tasks (%s) doesn't match total "
                                                                    "simulation count (%s). A task has failed." %
                                                                    (total, sim_count))
                                    err_file_name = "ERR [%s] - %s.txt" % (datetime.now().isoformat(), forecast_name)
                                    with open(err_file_name, mode='w') as err_file:
                                        err_file.write(''.join(stdout_lines))
                                    # Kill processes.
                                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)

                                if verbose:
                                    sys.stdout.write("\rRunning: %d. Completed: %02d/%02d. "
                                                     % (running, completed, total))
                                    sys.stdout.flush()

                    elif event & select.EPOLLHUP:
                        if fileno == p.stdout.fileno():
                            raise RuntimeError("Program ended.")

        except RuntimeError, err:
            print(err.message)
        finally:
            epoll.unregister(p.stdout.fileno())

        return p.poll()
