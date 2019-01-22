/**
 * Created by federico on 02/09/15.
 */

var modals = angular.module('modalsControllers', []);

modals.controller('ConfirmModalController', function ($scope, $modalInstance, action) {
    $scope.action = action;

    $scope.ok = function () {
        $modalInstance.close();
    };

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };
});


modals.controller('AddDateModalController', function ($scope, $modalInstance, action) {
    $scope.action = action;

    $scope.ok = function () {
        if($scope.control.date.$valid) {
            $modalInstance.close($scope.date);
        }
    };

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };
});