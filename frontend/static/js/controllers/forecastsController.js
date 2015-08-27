/**
 * Created by federico on 14/08/15.
 */

var forecasts = angular.module('forecastsModule', []);

forecasts.controller('forecastsController', function ($scope, $http, socket) {
    $scope.forecasts = [];

    $http.get('/api/forecasts').success(function (forecasts) {
        var _forecasts = {};
        angular.forEach(forecasts, function(forecast) {
            var file = forecast.file_name;
            if(!(file in _forecasts))
                _forecasts[file] = [];
            _forecasts[file].push(forecast);
        });
        $scope.forecasts = _forecasts;
    });

    $scope.$on('$destroy', function () {
        // clean up stuff
    })
});