angular.module("Dashboard", [ 'Config', '720kb.datepicker' ])
    .controller("dashboardCtrl", function($scope, $http, API_BASE) {
    	$scope.initDashboard = function() {
    		angular.element(document).ready(function() {
    			$('.menu .item').tab();
	    		$scope.pageNum = 0;
                $scope.isLoading = false;
                $('body').dimmer({ 'closable': false });
                $scope.outliers = [];
                $(window).scroll(function() {
                    if($(window).scrollTop() + $(window).height() == $(document).height()) {
                        if(!$scope.isLoading) {
                            $scope.getOutliers(($scope.pageNum) * 10);
                        }
                    }
                }) 
    		})
    		
    		$scope.selected_data = [];
    		$scope.getOutliers(0);
    	}

    	$scope.showUserDetail = function(data) {
    		$scope.currentUserData = {
    			id: data._source.log.user,
    			data: [],
    			startDate: moment(),
    			endDate: moment(),
    		}
    		$('#userModal').modal('show');
    		$scope.getUserData(data._source.log.user, moment().format("YYYY-MM-DD"), moment().format("YYYY-MM-DD"));
    	}

    	$scope.closeUserModal = function() {
    		$('#userModal').modal('close');
    		$scope.currentUserData = null;
    	}

    	$scope.getOutliers = function(from) {
            $scope.isLoading = true;
    		$http.get(API_BASE + "outliers/?from=" + from.toString())
    			.success(function(outliers) {
    				$scope.outliers = $scope.outliers.concat(outliers);
                    $scope.pageNum += 1;
                    $scope.isLoading = false;
    			})
    			.error(function() {
    				alert("error when retrieve outliers");
                    $scope.isLoading = false;
    			})
    	}

    	$scope.getUserData = function(userId, startDate, endDate) {
    		$http.get(API_BASE + "userData/" + userId + "/?startDate=" + startDate + "&endDate=" + endDate)
    			.success(function(data) {
    				$scope.currentUserData.data = $scope.currentUserData.data.concat(data.reverse());
    			})
    			.error(function() {
    				alert("error when retrieve userData");
    			})
    	}

    	$scope.getPreviousMonthData = function() {
    		var date = $scope.currentUserData.startDate;
    		var startDate = moment(date).subtract(1, 'months').format('YYYY-MM-DD');
    		var endDate = moment(date).subtract(1, 'days').format('YYYY-MM-DD');
    		date.subtract(1, 'months');
    		$scope.getUserData($scope.currentUserData.id, startDate, endDate);
    	}

        $scope.clearLabel = function(data) {
            data.label = null;
        }

        $scope.submitReport = function() {
            $('body').dimmer('show');
            var labeled_data = [];
            for(var i = 0; i < $scope.outliers.length; ++i) {
                var outlier = $scope.outliers[i];
                if(outlier.label != null) {
                    labeled_data.push(outlier);
                }
            }
            if(labeled_data.length == 0) {
                $('body').dimmer('hide');
                return;
            }
            $http.post(API_BASE + "labelData/", { "labeled_data": labeled_data })
                .success(function(res) {
                    alert("Server received labeled data, will update model later");
                    $('body').dimmer('hide');
                    location.reload();
                })
                .error(function() {
    				alert("error when sending label data");
                    $('body').dimmer('hide');
                })
        }
    })
    .filter("displayIP", function() {
    	return function(ip) {
    		return ip.split('_').join('.');
    	}
    })
    .filter("onlyDate", function() {
    	return function(dateObj) {
    		try {
    			return dateObj.format("YYYY/MM/DD");
    		} catch(err) {
    			return "";
    		}
    	}
    })
