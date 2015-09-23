/**
 * Created by federico on 14/08/15.
 */

var prindeApp = angular.module('prinde', [
    'ngRoute',
    'btford.socket-io',
    'luegg.directives',
    'ui.bootstrap',
    // controllers modules
    'logsModule',
    'jobQueueModule',
    'jobsModule',
    'sysConfigModule',
    'forecastsModule',
    'weatherModule',
    'modalsControllers'
]);

prindeApp.factory('socket', function (socketFactory) {
    if (!location.origin)
        location.origin = location.protocol + "//" + location.host;

    return socketFactory({
        prefix: '',
        ioSocket: io.connect(location.origin + '/observers')
    });
});

prindeApp.constant('jobsConstants', {
    status: {
        JOB_STATUS_RUNNING: 1,
        JOB_STATUS_WAITING: 2,
        JOB_STATUS_FINISHED: 3,
        JOB_STATUS_ERROR: 4,
        JOB_STATUS_INACTIVE: 5
    },
    status_description: {
        1: { css: 'label-info', text: 'Running'},
        2: { css: 'label-warning', text: 'Waiting'},
        3: { css: 'label-success', text: 'Finished'},
        4: { css: 'label-danger', text: 'Error'},
        5: { css: 'label-default', text: 'Inactive'},
        unknown: { css: 'label-default', text: 'Unknown'}
    }
});

prindeApp.constant('jobStatusClases', {});

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
        when('/weather', {
            templateUrl: 'static/partials/weather.html',
            controller: 'weatherController'
        }).
        when('/job/:jobId', {
            templateUrl: 'static/partials/job.html',
            controller: 'jobController'
        }).
        otherwise({
            redirectTo: '/'
        });

    $locationProvider.html5Mode(true);
}]);

prindeApp.run(function (socket, $rootScope) {
    $rootScope.backendConnected = false;
    $rootScope.Utils = {
        keys: function(obj) {
            if(obj !== null && obj !== undefined) return Object.keys(obj);
            return [];
        }
    };

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
