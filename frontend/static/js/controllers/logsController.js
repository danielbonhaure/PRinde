/**
 * Created by federico on 14/08/15.
 */

var logs = angular.module('logsModule', []);

logs.controller('logsController', function ($scope, $rootScope, socket) {

    //$scope.logs = [
    //    '2015-08-10 13:50:56,464 - main - main:57 - INFO - System startup.',
    //    '2015-08-10 13:50:56,723 - main - RainfallQuantiles:52 - INFO - Updated rainfall quantiles for stations [87649].'
    //];
    $scope.logs = [];
    $scope.min_log_level = 0;
    $scope.log_fullscreen = false;
    $rootScope.log_hidden = false;

    $scope.logFilter = function (obj) {
        return obj.level_int >= $scope.min_log_level;
    };

    socket.on('connect', function () {
        $scope.logs = [];
    });


    socket.on('logs', function (json) {
        //var lines = msg.split("\n");
        var $log = $('#log');

        angular.forEach(json, function (line) {
            line = line.trim();
            //if (line.length == 0) return;

            //line = line.replace(/\n/g, '<br>');

            var log_level = 'debug';
            var log_level_int = 0;

            if (line.indexOf('INFO') > -1) {
                log_level = 'info';
                log_level_int = 1;
            } else if (line.indexOf('WARN') > -1) {
                log_level = 'warn';
                log_level_int = 2;
            } else if (line.indexOf('ERR') > -1) {
                log_level = 'error';
                log_level_int = 3;
            }
            $scope.logs.push({
                level: log_level,
                level_int: log_level_int,
                message: line
            });
        });
        //$log.scrollTop($log[0].scrollHeight + 20);  // Scroll to bottom of component.
    });

});