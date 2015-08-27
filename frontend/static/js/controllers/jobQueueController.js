/**
 * Created by federico on 14/08/15.
 */

var jobQueue = angular.module('jobQueueModule', []);

jobQueue.controller('jobQueueController', function ($scope, $rootScope, socket) {

    $scope.job_queue = {};
    $scope.finished_tasks = {};
    $scope.active_jobs = {};

    console.log('jobQueueController loaded.');

    // If we're connected to the backend, we request the job queue and active tasks.
    if($rootScope.backendConnected) {
        socket.emit('get_tasks');
    }

    socket.on('connect', function () {
        $scope.job_queue = {};
        $scope.finished_tasks = {};
        socket.emit('get_tasks');
    });


    socket.on('tasks', function (json) {
        $scope.job_queue = json['job_queue'];
        $scope.finished_tasks = json['finished_tasks'];
    });


    var processEvent = function(e) {
        var job_id = e.job.id;
        if (!(job_id in $scope.active_jobs)) {
            $scope.active_jobs[job_id] = e
        }
        var completed = Math.max((e.current_value - e.start_value) / Math.max(e.end_value, 1) * 100, 0);
        $scope.active_jobs[job_id]['perc_completed'] = completed;
        $scope.active_jobs[job_id]['current_value'] = e.current_value;

        if(completed >= 100) {
            // Clear completed tasks after 5 seconds.
            setTimeout(function(){
                delete $scope.active_jobs[job_id];
                $scope.$apply();
            }, 5000)
        }
    };

    socket.on('active_tasks', function(tasks) {
        angular.forEach(tasks, function(event) {
            processEvent(event);
        });
    });

    socket.on('active_tasks_event', processEvent);

    socket.on('disconnect', function () {
        $scope.active_jobs = {};
    });
});