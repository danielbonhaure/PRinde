/**
 * Created by federico on 14/08/15.
 */

var conf = angular.module('sysConfigModule', []);

conf.controller('sysConfigController', function ($scope, $routeParams, $http, socket) {

    $scope.config = {};

    $http.get('/api/config').success(function (data) {
        var conf = {};
        angular.forEach(data, function (value, key) {
            if (typeof value === "object") {
                // Unwind nested objects.
                Object.keys(value).map(function (inner_key) {
                    conf[key + ' > ' + inner_key] = Object(value[inner_key]);
                });
            } else {
                conf[key] = Object(value);
            }
        });
        $scope.config = conf;
    });

    $scope.$on('$destroy', function () {
        // clean up stuff
    })
});