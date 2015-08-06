__author__ = 'Federico Schmidt'

from flask import Flask, render_template


class UI:
    app = Flask(__name__)

    def __init__(self, main):
        self.main = main
        self.app.config.update(DEBUG=True)
        self.app.add_url_rule('/', 'index', self.hello)
        self.app.add_url_rule('/active_tasks', 'active_tasks', self.list_tasks)

        pass

    def hello(self):
        return "Hello World!"

    def list_tasks(self):
        return render_template('pending_jobs.html', jobs=self.main.scheduler.get_jobs())

    def start(self):
        self.app.run()

