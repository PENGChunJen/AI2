angular.module('analystApp', ['elasticsearch', '720kb.datepicker', 'Config'])
    .service('client', function(esFactory, ES_HOST) {
        return esFactory({
            host: ES_HOST,
        })
    })
    .controller('outliersCtrl', function($scope, $location, client, esFactory, ES_INDEX) {
        $scope.displayTableConfig = {
            score: true,
            user: true,
            service: true,
            ip: false,
            device: true,
            geoInfo: true,
            timestamp: true,
            label: true,
        }
        $scope.logModal = {
            targetLog: null,
            display: false,
        }
        $scope.loadingModal = {
            text: "",
            display: false,
        }
        $scope.logMgr = {
            scrollId: null,
            logTotal: 0,
            logs: [],
            startDate: moment().subtract(7, 'days').format('YYYY-MM-DD'),
            endDate: moment().format('YYYY-MM-DD'),
            modified: false,
        }

        $scope.submitLabels = function() {
            updatedLogs = [];
            for(var i = 0; i < $scope.logMgr.logs.length; ++i) {
                if($scope.logMgr.logs[i].updatedLabel) {
                    var log = $scope.logMgr.logs[i]._source.log;
                    var logId = log.timestamp + "_" + log.user + "_" + log.service;
                    var labelType = $scope.logMgr.logs[i].updatedLabel;
                    updatedLogs.push({
                        update: {
                            _index: ES_INDEX,
                            _type: 'log',
                            _id: logId,
                        }
                    });
                    updatedLogs.push({
                        doc: {
                            "label": {
                                "analyst": labelType,
                            }
                        }
                    });

                    updatedLogs.push({
                        update: {
                            _index: ES_INDEX,
                            _type: 'data',
                            _id: logId,
                        }
                    });
                    updatedLogs.push({
                        doc: {
                            "label": {
                                "analyst": labelType,
                            },
                            "log": {
                                "label": {
                                    "analyst": labelType,
                                }
                            }
                        }
                    });
                }
            }
            if(updatedLogs.length == 0) {
                return 0;
            } else {
                $scope.loadingModal.text = "Submitting label results...";
                $scope.loadingModal.display = true;
                client.bulk({
                    body: updatedLogs,
                }, function(error, response) {
                    alert("Server receive the label result, the whole model will be updated later");
                    location.reload();
                })
            }
        }

        $scope.changeDateRange = function() {
            if ($scope.logMgr.modified) {
                var result = confirm("Change date range will clear the labels you labeled this time. Do you still want to change date range or cancel and submit the work first?");
                if (!result) {
                    return;
                }
            }
            $location.search('startDate', $scope.logMgr.startDate);
            $location.search('endDate', $scope.logMgr.endDate);
            $scope.logMgr.modified = false;
            $scope.logMgr.logs = [];
            $scope.scrollId = null;
            $scope.logTotal = 0;
            $scope.initIndex();
        }

        $scope.dealRetrievedLogs = function(error, response) {
            $scope.logMgr.logTotal = response.hits.total;
            $scope.logMgr.scrollId = response._scroll_id;
            $scope.logMgr.logs = $scope.logMgr.logs.concat(response.hits.hits);
            $scope.loadingModal.display = false;
        }

        $scope.initIndex = function() {
            $scope.loadingModal.text = "Retrieving data...";
            $scope.loadingModal.display = true;
            if($location.search().startDate) {
                $scope.logMgr.startDate = $location.search().startDate;
            }
            if($location.search().endDate) {
                $scope.logMgr.endDate = $location.search().endDate;
            }
            queryBody = {
                query: {
                    bool: {
                        filter: {
                            range: {
                                "log.timestamp": {
                                    from: $scope.logMgr.startDate,
                                    to: $scope.logMgr.endDate,
                                }
                            }
                        },
                        must_not: {
                            exists: {
                                field: "label.analyst"
                            }
                        },
                    }
                },
                sort: {
                    "scores.autoencoder": {
                        order: "desc",
                    }
                },
                size: 50,
            }

            var user = $location.search().user;
            if(user) {
                $scope.user = user;
                queryBody.query.bool.must = {
                    match: {
                        "log.user": user,
                    }
                }
                queryBody.sort = {
                    "log.timestamp": {
                        order: "desc",
                    }
                }
                delete queryBody.query.bool.must_not;
            }

            client.search({
                index: ES_INDEX,
                type: 'data',
                scroll: '10m',
                body: queryBody,
            }, $scope.dealRetrievedLogs);
        }

        $scope.loadMore = function() {
            $scope.loadingModal.text = "Retrieving more data...";
            $scope.loadingModal.display = true;
            client.scroll({
                scrollId: $scope.logMgr.scrollId,
                scroll: "10m",
            }, $scope.dealRetrievedLogs);
        }

        $scope.showLogDetail = function(log) {
            $scope.logModal.targetLog = log;
            $scope.logModal.display = true;
        }

        $scope.updateLable = function(log) {
            if(log._source.label.analyst == "")
                log._source.label.analyst = null;
            $scope.logMgr.modified = true;
            log.updatedLabel = log._source.label.analyst;
        }

        $scope.openUserTab = function(log) {
            var oriUser = $location.search().user;
            var oriEndDate = $location.search().endDate;

            $location.search('user', log._source.log.user);
            $location.search('endDate', log._source.log.timestamp);
            var win = window.open($location.absUrl(), '_blank');
            win.focus();

            $location.search('user', oriUser);
            $location.search('endDate', oriEndDate);
        }
    })
    .filter("displayIP", function() {
        return function(ip) {
            if (ip) {
                return ip.split('_').join('.');
            } else {
                return "";
            }
        }
    })
    .filter("trueFalse", function() {
        return function(val) {
            if (val) {
                if (val == "1") {
                    return "True";
                } else {
                    return "False";
                }
            } else {
                return "";
            }
        }
    })
