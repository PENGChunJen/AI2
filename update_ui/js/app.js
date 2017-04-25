angular.module('analystApp', ['elasticsearch', '720kb.datepicker', 'Config'])
    .service('client', function(esFactory, ES_HOST) {
        return esFactory({
            host: ES_HOST,
        })
    })
    .controller('outliersCtrl', function($scope, $location, $sce, client, esFactory, ES_INDEX) {
        $scope.displayTableConfig = {
            quickSelect: false,
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
            querySize: 50,
        }
        $scope.logFilter = {}
        $scope.shoControlPannel = true;
        $scope.currentTab = 'Table';

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

        $scope.initLogScroll = function() {
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

        $scope.genGeoVisualURL = function() {
            if($scope.user) {
                $scope.geo_visual_url = $sce.trustAsResourceUrl("http://163.28.17.48:5601/app/kibana#/visualize/create?embed=true&type=tile_map&indexPattern=production1&_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:'" + $scope.logMgr.startDate + "',mode:absolute,to:'" + $scope.logMgr.endDate + "'))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:log.user=" + $scope.user + ")),uiState:(mapCenter:!(14.944784875088372,0)),vis:(aggs:!((enabled:!t,id:'1',params:(),schema:metric,type:count),(enabled:!t,id:'2',params:(autoPrecision:!t,field:location),schema:segment,type:geohash_grid)),listeners:(),params:(addTooltip:!t,heatBlur:15,heatMaxZoom:16,heatMinOpacity:0.1,heatNormalizeData:!t,heatRadius:25,isDesaturated:!t,legendPosition:bottomright,mapCenter:!(15,5),mapType:'Scaled+Circle+Markers',mapZoom:2,wms:(enabled:!f,options:(attribution:'Maps+provided+by+USGS',format:image%2Fpng,layers:'0',styles:'',transparent:!t,version:'1.3.0'),url:'https:%2F%2Fbasemap.nationalmap.gov%2Farcgis%2Fservices%2FUSGSTopo%2FMapServer%2FWMSServer')),title:'New+Visualization',type:tile_map))");
                console.log($scope.user);
            } else {
                $scope.geo_visual_url = $sce.trustAsResourceUrl("http://163.28.17.48:5601/app/kibana#/visualize/create?embed=true&type=tile_map&indexPattern=production1&_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:'" + $scope.logMgr.startDate + "',mode:absolute,to:'" + $scope.logMgr.endDate + "'))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),uiState:(mapCenter:!(14.944784875088372,0)),vis:(aggs:!((enabled:!t,id:'1',params:(),schema:metric,type:count),(enabled:!t,id:'2',params:(autoPrecision:!t,field:location),schema:segment,type:geohash_grid)),listeners:(),params:(addTooltip:!t,heatBlur:15,heatMaxZoom:16,heatMinOpacity:0.1,heatNormalizeData:!t,heatRadius:25,isDesaturated:!t,legendPosition:bottomright,mapCenter:!(15,5),mapType:'Scaled+Circle+Markers',mapZoom:2,wms:(enabled:!f,options:(attribution:'Maps+provided+by+USGS',format:image%2Fpng,layers:'0',styles:'',transparent:!t,version:'1.3.0'),url:'https:%2F%2Fbasemap.nationalmap.gov%2Farcgis%2Fservices%2FUSGSTopo%2FMapServer%2FWMSServer')),title:'New+Visualization',type:tile_map))");
            }
        }

        $scope.initIndex = function() {
            $scope.loadingModal.text = "Retrieving data...";
            $scope.loadingModal.display = true;
            if($location.search().startDate) {
                $scope.logMgr.startDate = $location.search().startDate;
            } else {
                $location.search('startDate', $scope.logMgr.startDate);
            }
            if($location.search().endDate) {
                $scope.logMgr.endDate = $location.search().endDate;
            } else {
                $location.search('endDate', $scope.logMgr.endDate);
            }
            var queryBody = {
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
                        must: [],
                    }
                },
                sort: {
                    "scores.total": {
                        order: "desc",
                    }
                },
                size: 50,
            }

            var user = $location.search().user;
            if(user) {
                $scope.user = user;
                queryBody.query.bool.must = [{
                        match: {
                            "log.user": user,
                        }
                    }
                ]
                queryBody.sort = {
                    "log.timestamp": {
                        order: "desc",
                    }
                }
                delete queryBody.query.bool.must_not;
                $scope.displayTableConfig.user = false;
            }
            if($scope.logMgr.querySize) {
                queryBody.size = $scope.logMgr.querySize;
            }
            if(!angular.equals($scope.logFilter, {})) {
                var logFilter = $scope.logFilter;
                if(logFilter._source.log.service) {
                    queryBody.query.bool.must.push({
                        match: {
                            "log.service": logFilter._source.log.service,
                        }
                    });
                }
                if(logFilter._source.log.device) {
                    queryBody.query.bool.must.push({
                        match: {
                            "log.device": logFilter._source.log.device,
                        }
                    });
                }
                if(logFilter._source.log.IP) {
                    queryBody.query.bool.must.push({
                        match: {
                            "log.IP": logFilter._source.log.IP,
                        }
                    });
                }
                if(logFilter._source.log.city) {
                    queryBody.query.bool.must.push({
                        match: {
                            "log.city": logFilter._source.log.city,
                        }
                    })
                }
                if(logFilter._source.log.region) {
                    queryBody.query.bool.must.push({
                        match: {
                            "log.region": logFilter._source.log.region,
                        }
                    })
                }
                if(logFilter._source.log.country) {
                    queryBody.query.bool.must.push({
                        match: {
                            "log.country": logFilter._source.log.country,
                        }
                    })
                }
            }

            $scope.genGeoVisualURL();

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

        $scope.updateLabel = function(log) {
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

        $scope.localFilter = function() {
            if($scope.asLocalFilter) {
                $scope.localLogFilter = $scope.logFilter;
            } else {
                $scope.localLogFilter = {};
            }
        }

        $scope.selectAll = function() {
            if($scope.useQuickLabel) {
                for(var i = 0; i < $scope.logMgr.logs.length; ++i) {
                    $scope.logMgr.logs[i].quickLabelChecked = true;
                }
            } else {
                for(var i = 0; i < $scope.logMgr.logs.length; ++i) {
                    $scope.logMgr.logs[i].quickLabelChecked = false;
                }
            }
            $scope.updateAllLabel();
        }

        $scope.updateAllLabel = function() {
            for(var i = 0; i < $scope.logMgr.logs.length; ++i) {
                var log = $scope.logMgr.logs[i];
                if(log.quickLabelChecked) {
                    if(log._source.label.analyst == "") {
                        log._source.label.analyst = null;
                    } else {
                        log._source.label.analyst = $scope.quickLabel;
                    }
                    log.updatedLabel = log._source.label.analyst;
                }
            }
            $scope.logMgr.modified = true;
        }

        $scope.changeLabel = function(log) {
            if(log.quickLabelChecked) {
                log._source.label.analyst = $scope.quickLabel;
                if(log._source.label.analyst == "") {
                    log._source.label.analyst = null;
                } else {
                    log._source.label.analyst = $scope.quickLabel;
                }
                log.updatedLabel = log._source.label.analyst;
            }
            $scope.logMgr.modified = true;
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
    .filter("deviceDisplay", function() {
        return function(val) {
            var str = '';
            angular.forEach(val, function(value, key) {
                str += (key + ': ' + value + ', ');
            })
            return str;
        }
    })
