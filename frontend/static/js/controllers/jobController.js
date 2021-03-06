/**
 * Created by federico on 14/08/15.
 */

var job = angular.module('jobsModule', []);

job.controller('jobController', function ($scope, $rootScope, $routeParams, $modal, $http, $location, socket, jobsConstants) {
    $scope.jobId = $routeParams.jobId;
    $scope.task_details = {};
    $scope.sub_jobs = {};


    if($rootScope.backendConnected) {
        socket.emit('get_job_details', $scope.jobId);
    }

    //$rootScope.$broadcast('unload_jobQueue');

    var onConnected = function () {
        $scope.task_details = {};
        $scope.sub_jobs = {};
        socket.emit('get_job_details', $scope.jobId);
    };

    var processEvent = function(e) {
        if (e === null)
            return;

        console.log(e);

        var task_details = $scope.task_details;
        var job_id = e.job.id;
        var parent_job_id = e.job.parent;

        // This event doesn't correspond to the current job.
        if (parent_job_id != $scope.jobId && job_id != $scope.jobId)
            return;

        if(parent_job_id != null) {
            $scope.sub_jobs[job_id] = e;
            task_details = $scope.sub_jobs[job_id];
        } else {
            $scope.task_details = e;
            task_details = $scope.task_details;
        }

        var completed = Math.max((e.current_value - e.start_value) / Math.max(e.end_value, 1) * 100, 0);
        task_details['perc_completed'] = completed;
        task_details['current_value'] = e.current_value;
        if (task_details.job.status in jobsConstants.status_description) {
            task_details.status = jobsConstants.status_description[task_details.job.status];
            task_details.can_be_modified = false;
        } else {
            task_details.status = jobsConstants.status_description[jobsConstants.status.JOB_STATUS_INACTIVE];
            task_details.can_be_modified = true;
        }

        $scope.$apply();
    };

    var jobDetails = function (json) {
        if (json === null) return; // Job not found.

        $scope.task_details = json;
        $scope.sub_jobs = {};
        processEvent(json);
        if('sub_jobs' in json) {
            angular.forEach(json['sub_jobs'], function(event) {
                processEvent(event)
            });
        }
    };

    socket.on('connect', onConnected);
    socket.on('job_details', jobDetails);
    socket.on('active_tasks_event', processEvent);

    $scope.showConfirmRescheduleModal = function (job_id) {

        var modalInstance = $modal.open({
            templateUrl: 'static/partials/modals/confirm.html',
            controller: 'ConfirmModalController',
            resolve: {
                action: function() {
                    return 'run job "' + job_id + '" now'
                }
            }
        });

        modalInstance.result.then(function () {
            $http.get('/api/job/run_now/' + job_id).success(function () {
                $location.path('/');
            });
        });
    };

    $scope.showConfirmDeleteModal = function (job_id) {

        var modalInstance = $modal.open({
            templateUrl: 'static/partials/modals/confirm.html',
            controller: 'ConfirmModalController',
            resolve: {
                action: function() {
                    return 'cancel job "' + job_id + '"'
                }
            }
        });

        modalInstance.result.then(function () {
            $http.get('/api/job/cancel/' + job_id).success(function () {
                $location.path('/');
            });
        });
    };

    $scope.$on('$destroy', function () {
        socket.removeListener('connect', onConnected);
        socket.removeListener('job_details', jobDetails);
        socket.removeListener('active_tasks_event', processEvent);
    });
});