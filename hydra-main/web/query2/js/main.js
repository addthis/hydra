/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

require.config({
    baseUrl: "js",
    paths: {
        "jquery": "../../spawn2/js/vendor/jquery-1.9.1"
        ,"backbone": "../../spawn2/js/vendor/backbone"
        ,"underscore": "../../spawn2/js/vendor/underscore"
        ,"bootstrap":"../../spawn2/js/vendor/bootstrap"
        ,"jquery.dataTable":"../../spawn2/js/vendor/jquery.dataTables.nightly"
        ,"dataTable.scroller":"../../spawn2/js/vendor/dataTables.scroller"
        ,"domReady":"../../spawn2/js/vendor/domReady"
        ,"date":"../../spawn2/js/vendor/date"
        ,"d3":"../../spawn2/js/vendor/d3.v2"
        ,"nvd3":"../../spawn2/js/vendor/js/nv.d3"
        ,"json":"../../spawn2/js/vendor/json"
        ,"text":"../../spawn2/js/vendor/text"
        ,"jquery.cookie":"../../spawn2/js/vendor/jquery.cookie"
        ,"alertify":"../../spawn2/js/vendor/alertify"
        ,"queryData":"/v2/queries/list"
    },
    shim: {
        'backbone': {
            deps: ['underscore', 'jquery'],
            exports: 'Backbone'
        },
        'underscore': {
            exports: '_'
        },
        'localstorage':{
            deps:['backbone']
        },
        "jquery":{
            exports:'jQuery'
        },
        "bootstrap":{
            deps:["jquery"]
        },
        "d3":{
            deps:[],
            exports:"d3"
        },
        "nvd3":{
            deps:["d3"],
            exports:"nv"
        },
        "jquery.dataTable":['jquery'],
        'jquery.cookie':['jquery'],
        "dataTable.scroller":['jquery.dataTable']
    },
    packages: []
});
require([
    "app",
    "modules/queries",
    "modules/mqworkers",
    "modules/query.jobs",
    "modules/metrics",
    "modules/git",
    "../../spawn2/js/modules/layout.views",
    "json!queryData",
    "domReady",
    "backbone",
    "underscore",
    "jquery",
    "bootstrap",
    "date"
],
function(
    app,
    Queries,
    MQWorker,
    Jobs,
    Metrics,
    Git,
    Layout,
    queryData,
    domReady
){
    app.queryCollection = new Queries.Collection(
        Queries.Collection.prototype.parse(queryData)
    );
    app.mqworkerCollection = new MQWorker.Collection();
    app.jobsCollection = new Jobs.Collection();
    app.server.connect();
    app.activeModels = {};
    app.router.on("route:showIndex",function(){
        app.router.navigate("#queries",{trigger:true});
    });
    app.router.on("route:showQueryDetails",function(queryId){
        var table = new Queries.TableView({
            collection:app.queryCollection,
            compact:true,
            id:"tinyQueryTable",
            breadcrumbTemplate:"<ul class='breadcrumb pull-left'><li><a href='#queries'>All Queries</a></li></ul>"
        });
        var model = app.queryCollection.get(queryId);
        var queryHostCollection = new Queries.HostCollection();
        queryHostCollection.uuid=queryId;
        var detail = new Queries.DetailView({
            model:model,
            collection:queryHostCollection
        });
        var view = new Layout.VerticalSplit({
            rightView:detail,
            leftView:table,
            rightWidth:85,
            leftWidth:15
        });
        app.queryCollection.fetch();
        queryHostCollection.fetch();
        app.activeModels={
            "queryCollection":app.queryCollection,
            "queryHostCollection":queryHostCollection
        };
        app.showView(view,"#queries");
    });
    app.router.on("route:showQueriesTable",function(){
        var view = new Queries.TableView({
            collection:app.queryCollection,
            compact:false,
            id:"queryTable"
        });
        app.queryCollection.fetch();
        app.activeModels={
            "queryCollection":app.queryCollection
        };
        app.showView(view,"#queries");
    });
    app.router.on("route:showHostTable",function(){
        var view = new MQWorker.TableView({
            collection:app.mqworkerCollection
        });
        app.mqworkerCollection.fetch();
        app.activeModels={
            "mqworkerCollection":app.mqworkerCollection
        };
        app.showView(view,"#hosts");
    });
    app.router.on("route:showJobTable",function(){
        var view = new Jobs.TableView({
            collection:app.jobsCollection
        });
        app.jobsCollection.fetch();
        app.activeModels={
            "jobsCollection":app.jobsCollection
        };
        app.showView(view,"#jobs");
    });
    app.router.on("route:showMetrics",function(){
        var view = new Metrics.View({
        });
        app.showView(view,"#metrics");
    });
    app.router.on("route:showGitProperties",function(){
        var model = new Git.Model();
        var view = new Git.PropertiesView({
            model:model
        });
        model.fetch();
        app.showView(view,"#git");
        //app.makeHtmlTitle("Git");
    });
    app.user.on("change:username",function(){
        $("#usernameBox").html(app.user.get("username"));
        $.ajaxSetup({
            global:true,
            headers:{
                "Username":app.user.get("username")
            }
        });
    });
    app.authenticate();
    domReady(function(){
        Backbone.history.start();

        setInterval(function(){
            if(!_.isEmpty(app.activeModels)){
                var values = _.values(app.activeModels);
                _.each(values,function(model){
                    model.fetch();
                });
            }
        },2000);
    });
    window.app=app;
});