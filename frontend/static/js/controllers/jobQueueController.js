/**
 * Created by federico on 14/08/15.
 */

var jobQueue = angular.module('jobQueueModule', []);

jobQueue.controller('jobQueueController', function ($scope, $rootScope, socket, jobsConstants) {

    $scope.job_queue = {};
    $scope.finished_tasks = {};
    $scope.active_jobs = {};

    // If we're connected to the backend, we request the job queue and active tasks.
    if($rootScope.backendConnected) {
        socket.emit('get_tasks');
    }

    //$rootScope.$broadcast('unload_jobController');

    var onConnect = function () {
        $scope.job_queue = {};
        $scope.finished_tasks = {};
        socket.emit('get_tasks');
    };


    var tasksReceived = function (json) {
        $scope.job_queue = json['job_queue'];
        $scope.finished_tasks = json['finished_tasks'];
    };

    var processEvent = function(e) {
        var active_jobs = $scope.active_jobs;
        var job_id = e.job.id;
        var parent_job_id = e.job.parent;

        if(parent_job_id != null) {
            if (!(parent_job_id in active_jobs))
                return;

            var parent_job = active_jobs[parent_job_id];
            if(!("sub_jobs" in parent_job)){
                parent_job.sub_jobs = {}
            }
            // Change the active_jobs array to the job's inner sub jobs array.
            active_jobs = $scope.active_jobs[parent_job_id].sub_jobs;
        }

        if (!(job_id in active_jobs)) {
            e.sub_jobs = {};
            active_jobs[job_id] = e
        }
        var completed = Math.max((e.current_value - e.start_value) / Math.max(e.end_value, 1) * 100, 0);
        active_jobs[job_id]['perc_completed'] = completed;
        active_jobs[job_id]['current_value'] = e.current_value;
        active_jobs[job_id]['end_value'] = e.end_value;

        if (e.job.status in jobsConstants.status_description) {
            active_jobs[job_id].status = jobsConstants.status_description[e.job.status];
        } else {
            active_jobs[job_id].status = jobsConstants.status_description[jobsConstants.status.JOB_STATUS_INACTIVE];
        }

        $scope.$apply();

        if(completed >= 100) {
            // Clear completed tasks after 10 seconds.
            setTimeout(function(){
                delete active_jobs[job_id];
                $scope.$apply();
            }, 10000)
        }
    };

    var processActiveTasks = function(tasks) {
        angular.forEach(tasks, function(event) {
            processEvent(event);
        });
    };

    socket.on('connect', onConnect);
    socket.on('tasks', tasksReceived);
    socket.on('active_tasks', processActiveTasks);
    socket.on('active_tasks_event', processEvent);

    socket.on('disconnect', function () {
        $scope.active_jobs = {};
    });

    //$rootScope.$on('unload_jobQueue', function() {
    //    $scope.$destroy();
    //});

    $scope.$on('$destroy', function () {
        socket.removeListener('connect', onConnect);
        socket.removeListener('tasks', tasksReceived);
        socket.removeListener('active_tasks', processActiveTasks);
        socket.removeListener('active_tasks_event', processEvent);
    });
});