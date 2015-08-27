/**
 * Created by federico on 14/08/15.
 */

var prindeApp = angular.module('prinde', [
    'ngRoute',
    'btford.socket-io',
    'luegg.directives',
    // controllers modules
    'logsModule',
    'jobQueueModule',
    'jobsModule',
    'sysConfigModule',
    'forecastsModule'
]);

prindeApp.factory('socket', function (socketFactory) {
    return socketFactory({
        prefix: '',
        ioSocket: io.connect('http://localhost:5000/observers')
    });
});

prindeApp.config(['$routeProvider', '$locationProvider', function ($routeProvider, $locationProvider) {
    // Configure app routes.
    $routeProvider.
        when('/', {
            templateUrl: 'static/partials/job_queue.html',
            controller: 'jobQueueController'
        }).
        when('/config', {
            templateUrl: 'static/partials/system_config.html',
            controller: 'sysConfigController'
        }).
        when('/forecasts', {
            templateUrl: 'static/partials/forecasts.html',
            controller: 'forecastsController'
        }).
        when('/job/:jobId', {
            templateUrl: 'static/partials/job.html',
            controller: 'jobController'
        }).
        otherwise({
            redirectTo: '/'
        });

    $locationProvider.html5Mode( true );
}]);

prindeApp.run(function (socket, $rootScope) {
    $rootScope.backendConnected = false;

    socket.on('connect', function () {
        $rootScope.backendConnected = true;
        socket.emit('connected');
    });

    socket.on('disconnect', function () {
        $rootScope.backendConnected = false;
    });
});

// Filter to change dictionaries to arrays.
prindeApp.filter('toArray', function () {
    return function (obj, addKey) {
        if (!(obj instanceof Object)) {
            return obj;
        }

        if (addKey === false) {
            return Object.values(obj);
        } else {
            return Object.keys(obj).map(function (key) {
                return Object.defineProperty(obj[key], '$key', {enumerable: false, value: key});
            });
        }
    };
});