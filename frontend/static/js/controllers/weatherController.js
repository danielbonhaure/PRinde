/**
 * Created by federico on 14/08/15.
 */

var conf = angular.module('weatherModule', []);

conf.controller('weatherController', function ($scope, $routeParams, $http, $modal, $location) {

    $scope.weather_data = {};

    $http.get('/api/weather_data').success(function (data) {
        var weather_data = {};
        // Key, value is inverted on purpose.
        angular.forEach(data, function (key, value) {
            if(!(key in weather_data)) {
                weather_data[key] = Object(value);
            } else{
                weather_data[key] = Object(weather_data[key].toString() + ', ' + value);
            }
        });
        $scope.weather_data = weather_data;
    });
});

