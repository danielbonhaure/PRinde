/**
 * Created by federico on 14/08/15.
 */

var forecasts = angular.module('forecastsModule', []);

forecasts.controller('forecastsController', function ($scope, $http, $modal, $location) {
    $scope.forecasts = [];

    $scope.getForecasts = function () {
        $http.get('/api/forecasts').success(function (forecasts) {
            var _forecasts = {};
            angular.forEach(forecasts, function (forecast) {
                var file = forecast.file_name;
                if (!(file in _forecasts))
                    _forecasts[file] = [];
                _forecasts[file].push(forecast);
            });
            $scope.forecasts = _forecasts;
        });
    };

    $scope.getForecasts();

    $scope.showConfirmModal = function (forecast_file) {

        var modalInstance = $modal.open({
            templateUrl: 'static/partials/modals/confirm.html',
            controller: 'ConfirmModalController',
            resolve: {
                action: function() {
                    return 'reload forecast file "' + forecast_file + '"'
                }
            }
        });

        modalInstance.result.then(function () {
            //console.log('Reload file ' + forecast_file);
            $http.get('/api/forecasts/reload/' + forecast_file).success(function () {
                $location.path('/');
            });
        });
    };

    $scope.$on('$destroy', function () {
        // clean up stuff
    })
});