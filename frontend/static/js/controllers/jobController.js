/**
 * Created by federico on 14/08/15.
 */

var job = angular.module('jobsModule', []);

job.controller('jobController', function ($scope, $routeParams, socket) {
    $scope.jobId = $routeParams.jobId;



    $scope.$on('$destroy', function () {
        // clean up stuff
    })
});