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
        "jquery": "./vendor/jquery-1.9.1"
        ,"backbone": "./vendor/backbone"
        ,"underscore": "./vendor/underscore"
        ,"bootstrap":"./vendor/bootstrap"
        ,"jquery.dataTable":"./vendor/jquery.dataTables.nightly"
        ,"dataTable.scroller":"./vendor/dataTables.scroller"
        ,"domReady":"./vendor/domReady"
        //,"jquery.resize":"./vendor/jquery.ba-resize"
        ,"date":"./vendor/date"
        ,"setupData":"/update/setup"
        ,"d3":"./vendor/d3.v2"
        ,"nvd3":"./vendor/js/nv.d3"
        ,"json":"./vendor/json"
        ,"text":"./vendor/text"
        ,"localstorage":"./vendor/backbone.localStorage"
        ,"jquery.cookie":"./vendor/jquery.cookie"
        ,"alertify":"./vendor/alertify"
        ,"ace":"./vendor/ace"
        ,"js-mode":"./vendor/mode-javascript"
        ,"js-worker":"./vendor/ace/worker/worker"
        ,"ace/ext/searchbox":"./vendor/ace/ext/searchbox"
        ,"git.template":"../../templates/git.properties.html"
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
        'bootstrap':{
            deps:['jquery']
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
        //"jquery.resize":['jquery'],
        'jquery.cookie':['jquery'],
        "dataTable.scroller":['jquery.dataTable']
    },
    packages: [
        {
            name: 'ace',
            location: './vendor/ace/ace',
            main: 'ace'
        }
    ]
});
require([
    "app",
    "router",
    "modules/jobs",
    "modules/macro",
    "modules/alias",
    "modules/alerts",
    "modules/command",
    "modules/host",
    "modules/layout.views",
    "modules/task",
    "modules/task.log",
    "modules/graph",
    "modules/git",
    "modules/alerts",
    "modules/settings",
    "modules/datatable",
    "json!setupData",
    "domReady",
    "backbone",
    "underscore",
    "jquery",
    "bootstrap",
    "date"
],
function(
    app,
    Router,
    Jobs,
    Macro,
    Alias,
    Alert,
    Command,
    Host,
    Layout,
    Task,
    TaskLog,
    Graph,
    Git,
    Alerts,
    Settings,
    DataTable,
    setupData,
    domReady,
    Backbone,
    _,
    $
){
    app.jobCollection = new Jobs.Collection(
        Jobs.Collection.prototype.parse(setupData.jobs)
    );
    app.hostCollection = new Host.Collection(
        Host.Collection.prototype.parse(_.values(setupData.hosts))
    );
    app.server.connect();
    app.commandCollection=new Command.Collection().reset(
        Command.Collection.prototype.parse(_.values(setupData.commands))
    );
    app.macroCollection=new Macro.Collection().reset(
        Macro.Collection.prototype.parse(_.values(setupData.macros))
    );
    app.aliasCollection=new Alias.Collection().reset(
        Alias.Collection.prototype.parse(_.values(setupData.aliases))
    );
    app.alertCollection=new Alert.Collection().reset(
    	Alert.Collection.prototype.parse(_.values(setupData.alerts))
    );
    app.jobInfoMetricModel = new Jobs.InfoMetricModel({});
    app.router.on("route:showIndex",function(){
        app.router.navigate("jobs",{trigger:true});
    });
    app.router.on("route:showJobsTable",function(){
        app.trigger("loadJobTable");
        app.showView(app.jobTable,"#jobs",["configModel","parameterCollection","job"])
        app.makeHtmlTitle("Jobs");
    });
    app.router.on("route:showJobCompactTable",function(){
        app.trigger("loadJobCompactTable");
        app.showView(app.jobTable,"#jobs",["configModel","parameterCollection","job"]);
        app.makeHtmlTitle("Jobs");
        //app.jobTable.resize();
    });
    app.router.on("route:showJobComfyTable",function(){
        app.trigger("loadJobComftTable");
        app.showView(app.jobTable,"#jobs",["configModel","parameterCollection","job"]);
        app.makeHtmlTitle("Jobs");
        //app.jobTable.resize();
    });
    app.router.on("route:showQuickTask",function(jobId){
        app.trigger("loadJobTable");
        app.job = app.jobCollection.get(jobId);
        var taskCollection = new Task.Collection();
        taskCollection.jobUuid=jobId;
        var taskTable = new Task.TableView({
            id:"taskTable"+jobId,
            collection:taskCollection
        });
        var dividerView = new Jobs.TaskDividerView({
            model:app.job
        });
        var layout = new Layout.HorizontalDividedSplit({
            topView:app.jobTable,
            bottomView:taskTable,
            dividerView:dividerView,
            topHeight:48,
            bottomHeight:48,
            dividerHeight:4
        });
        app.showView(layout,"#jobs");
        taskCollection.fetch({
            reset:true
        });
        app.makeHtmlTitle("Quick::"+jobId);
    });
    app.router.on("route:showQuickTaskDetail",function(jobId,node){
        app.trigger("loadJobTable");
        app.job = app.jobCollection.get(jobId);
        var taskCollection = new Task.Collection();
        taskCollection.jobUuid=jobId;
        taskCollection.fetch({
            reset:true
        });
        var task= new Task.Model({node:node,jobUuid:jobId});
        var log = new TaskLog.Model({
            jobUuid:jobId,
            node:node
        });
        var detail = new Task.DetailView({
            model:task,
            log:log
        });
        task.fetch({
            success:function(){
                task.trigger("reset");
            }
        });
        var taskTable = new Task.TinyTableView({
            id:"taskTable"+jobId,
            collection:taskCollection,
            model:task,
            nodeNumber:node,
            enableSearch:false
        });
        var bottomView = new Layout.VerticalSplit({
            rightView:detail,
            leftView:taskTable,
            rightWidth:80,
            leftWidth:20
        });
        var dividerView = new Jobs.TaskDetailDividerView({
            model:app.job,
            collection: taskCollection,
            nodeNumber:node
        });
        var layout = new Layout.HorizontalDividedSplit({
            topView:app.jobTable,
            bottomView:bottomView,
            dividerView:dividerView,
            topHeight:48,
            bottomHeight:48,
            dividerHeight:4
        });
        app.showView(layout,"#jobs");
        app.makeHtmlTitle("Job::"+jobId);
    });
    app.router.on("route:showHostTable",function(){
        var table = new Host.TableView({
            id:"hostTable",
            collection:app.hostCollection
        });
        app.showView(table,"#hosts");
        app.makeHtmlTitle("Hosts");
    });
    app.router.on("route:showMacroTable",function(){
        app.macroCollection.fetch();
        var table = new Macro.TableView({
            id:"macroTable",
            collection:app.macroCollection
        });
        app.showView(table,"#macros");
        app.makeHtmlTitle("Macros");
    });
    app.router.on("route:showCommandTable",function(){
        app.commandCollection.fetch();
        var table = new Command.TableView({
            id:"commandTable",
            collection:app.commandCollection
        });
        app.showView(table,"#commands");
        app.makeHtmlTitle("Commands");
    });
    app.router.on("route:showAliasTable",function(){
        app.aliasCollection.fetch();
        var table = new Alias.TableView({
            id:"aliasTable",
            collection:app.aliasCollection
        });
        app.showView(table,"#alias");
        app.makeHtmlTitle("Alias");
    });
    app.router.on("route:showJobConf",function(jobId){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var view = new Jobs.ConfDetailView({
                model:app.job,
                configModel:app.configModel,
                parameterCollection:app.parameterCollection,
                commandCollection:app.commandCollection
            });
            app.commandCollection.fetch();
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobConfClone",function(jobId){
        app.trigger("cloneJob",jobId);
        if(!_.isUndefined(app.job)){
            var view = new Jobs.ConfDetailView({
                                                   model:app.job,
                                                   isClone:true,
                                                   configModel:app.configModel,
                                                   parameterCollection:app.parameterCollection,
                                                   commandCollection:app.commandCollection
                                                   });
            app.commandCollection.fetch();
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobSettings",function(jobId){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var view = new Jobs.SettingDetailView({
                                                          model:app.job,
                                                          configModel:app.configModel,
                                                          parameterCollection:app.parameterCollection
                                                          });
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobSettingsClone",function(jobId){
        app.trigger("cloneJob",jobId);
        if(!_.isUndefined(app.job)){
            var view = new Jobs.SettingDetailView({
                                                      model:app.job,
                                                      isClone:true,
                                                      configModel:app.configModel,
                                                      parameterCollection:app.parameterCollection
                                                      });
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobAlerts",function(jobId){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var view = new Jobs.AlertDetailView({
                                                    model:app.job,
                                                    configModel:app.configModel,
                                                    parameterCollection:app.parameterCollection
                                                    });
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobAlertsClone",function(jobId){
        app.trigger("cloneJob",jobId);
        if(!_.isUndefined(app.job)){
            var view = new Jobs.AlertDetailView({
                                                    model:app.job,
                                                    isClone:true,
                                                    configModel:app.configModel,
                                                    parameterCollection:app.parameterCollection
                                                    });
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobDeps",function(jobId){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var graphModel = new Graph.TreeGraphModel({
                                                          jobId: jobId
                                                          });
            var view = new Jobs.DependenciesDetailView({
                                                           model:app.job,
                                                           configModel:app.configModel,
                                                           parameterCollection:app.parameterCollection,
                                                           graphModel:graphModel
                                                           });
            graphModel.fetch({reset:true});
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobExpConf",function(jobId){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var expandModel = new Jobs.ExpandedConfigModel();
            if(!app.job.isNew()){
                                    expandModel.set("id",app.job.id);
                                    }
            expandModel.set("config",app.configModel.get("config"));
            var view = new Jobs.ExpandedConfDetailView({
                                                           model:app.job,
                                                           configModel:app.configModel,
                                                           parameterCollection:app.parameterCollection,
                                                           expandModel:expandModel
                                                           });
            _.each(app.parameterCollection.toJSON(),function(param){
                                                                       expandModel.set("sp_"+param.name,param.value);
                                                                       });
            expandModel.fetch();
            if(app.parameterCollection.length===0){
                                                      app.parameterCollection.fetch();
                                                      }
            app.showView(view,"#jobs");
        }
    });
    app.router.on("route:showJobHistory",function(jobId){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var history = new Jobs.HistoryCollection();
            history.jobUuid=jobId;
            var view = new Jobs.HistoryDetailView({
                                                      model:app.job,
                                                      configModel:app.configModel,
                                                      parameterCollection:app.parameterCollection,
                                                      historyCollection:history
                                                      });
            app.showView(view,"#jobs");
            history.fetch();
        }
    });
    app.router.on("route:showJobHistoryView",function(jobId,commit){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var history = new Jobs.HistoryCollection();
            history.jobUuid=jobId;
            var commit = new Jobs.HistoryModel({
                                                   jobUuid:jobId,
                                                   commit:commit
                                                   });
            var view = new Jobs.HistoryCommitView({
                                                      model:app.job,
                                                      configModel:app.configModel,
                                                      parameterCollection:app.parameterCollection,
                                                      historyCollection:history,
                                                      commitModel:commit
                                                      });
            app.showView(view,"#jobs");
            history.fetch();
            commit.load().done(function(data){
                commit.set("historyConfig",data);
            }).fail(function(xhr){
                Alertify.dialog.error("Error loading commit: "+xhr.responseText);
            });
        }
    });
    app.router.on("route:showJobHistoryDiff",function(jobId,commit){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            var history = new Jobs.HistoryCollection();
            history.jobUuid=jobId;
            var commit = new Jobs.HistoryModel({
                                                   jobUuid:jobId,
                                                   commit:commit
                                                   });
            var view = new Jobs.HistoryCommitView({
                                                      model:app.job,
                                                      configModel:app.configModel,
                                                      parameterCollection:app.parameterCollection,
                                                      historyCollection:history,
                                                      commitModel:commit,
                                                      editorAttribute:"diff"
                                                      });
            app.showView(view,"#jobs");
            history.fetch();
            commit.diff().done(function(data){
                                                 commit.set("diff",data);
                                                 }).fail(function(xhr){
                Alertify.dialog.error("Error loading diff: "+xhr.responseText);
            });
        }
    });
    app.router.on("route:showJobTaskDetail",function(jobId,node){
        app.trigger("loadJob",jobId);
        if(!_.isUndefined(app.job)){
            if(_.isUndefined(app.job)){
                                          app.router.navigate("jobs",{trigger:true});
                                          }
            else if(parseInt(node)>=app.job.get("nodes")){
                                                             app.router.navigate("jobs/"+jobId+"/tasks");
                                                             }
            else{
                var taskCollection = new Task.Collection();
                taskCollection.jobUuid=jobId;
                taskCollection.fetch({
                                         reset:true
                                         });
                var task= new Task.Model({node:node,jobUuid:jobId});
                var logModel = new TaskLog.Model({
                                                     jobUuid:jobId,
                                                     node:node
                                                     });
                var detail = new Jobs.TaskDetailView({
                                                         model:app.job,
                                                         configModel:app.configModel,
                                                         parameterCollection:app.parameterCollection,
                                                         taskModel:task,
                                                         logModel:logModel,
                                                         taskCollection:taskCollection
                                                         });
                task.fetch({
                               success:function(){
                               task.trigger("reset");
                               }
                               });
                app.showView(detail,"#jobs");
            }
        }
    });
    app.router.on("route:showMacroDetail",function(name){
        var macro;
        if(_.isEqual(name,"create")){
            macro = new Macro.Model({});
            app.makeHtmlTitle("New Macro");
        }
        else{
            macro = app.macroCollection.get(name).clone();
            macro.fetch();
            app.makeHtmlTitle("Macro::"+name);
        }
        var view = new Macro.DetailView({
            model:macro
        });
        app.showView(view,"#macros");
    });
    app.router.on("showCheckDirs",function(jobId){
        app.trigger("loadJob",jobId);
        var collection = new Jobs.CheckDirsCollection({});
        collection.jobUuid=jobId;
        new Jobs.CheckDirsModal({
            collection:collection,
            model:app.job
        }).render();
        collection.fetch();
    });
    app.router.on("route:showJobBackups",function(jobId,node){
        app.trigger("loadJob",jobId);
        var backupModel = new Jobs.BackupModel({
            jobUuid:jobId,
            node:(!_.isUndefined(node)?node:-1)
        });
        new Jobs.BackupModalView({
            model:app.job,
            backupModel:backupModel
        }).render();
        backupModel.fetch();
    });
    app.router.on("route:showJobTaskTable",function(jobId){
        app.trigger("loadJob",jobId);
        var taskCollection = new Task.Collection();
        taskCollection.jobUuid=jobId;
        var view = new Jobs.TaskTableView({
            id:"taskTable"+jobId,
            model:app.job,
            configModel:app.configModel,
            parameterCollection:app.parameterCollection,
            taskCollection:taskCollection
        });
        app.showView(view,"#jobs");
        taskCollection.fetch({
            reset:true
        });
    });
    app.router.on("route:showAliasDetail",function(name){
        var alias;
        if(_.isEqual(name,"create")){
            alias = new Alias.Model({});
        }else{
            alias = app.aliasCollection.get(name);
        }
        var view = new Alias.DetailView({
            model:alias
        });
        app.showView(view,"#alias");
        app.makeHtmlTitle("Alias::"+name);
    });
    app.router.on("route:showCommandDetail",function(name){
        var command;
        if(_.isEqual(name,"create")){
            command = new Command.Model({});
        }else{
            command = app.commandCollection.get(name);
        }
        var view = new Command.DetailView({
            model:command
        });
        app.showView(view,"#commands");
        app.makeHtmlTitle("Command::"+name);
    });
    app.router.on("route:showHostTaskDetail",function(name){
        var host = app.hostCollection.get(name);
        var tasks = new Task.Collection();
        _.each(_.flatten([
            host.get('running'),
            host.get('queued'),
            host.get('backingup'),
            host.get('replicating')
        ]),function(taskData){
            var task = new Task.Model(
                Task.Model.prototype.parse({
                    jobUuid:taskData.jobUuid,
                    node: ""+taskData.nodeNumber,
                    hostUuid:name
                })
            );
            tasks.add(task);
            task.fetch();
        });
        var view = new Host.TaskDetailView({
            model:host,
            collection:tasks
        });
        app.showView(view,"#hosts");
        app.makeHtmlTitle("Host::"+name);
    });
    app.router.on("route:showRebalanceParams",function(){
        var model = new Settings.RebalanceModel();
        var view = new Settings.RebalanceView({
            model:model
        });
        model.fetch();
        app.showView(view,"#rebalanceParams");
        app.makeHtmlTitle("Rebalance Params");
    });
    app.router.on("route:showGitProperties",function(){
        var model = new Git.Model();
        var view = new Git.PropertiesView({
            model:model
        });
        model.fetch();
        app.showView(view,"#git");
        app.makeHtmlTitle("Git");
    });
    app.router.on("route:showAlertsTable",function(){
        app.alertCollection.fetch();
        var table = new Alerts.TableView({
            id:"alertTable",
            collection:app.alertCollection
        });
        app.showView(table,"#alert");
        app.makeHtmlTitle("Alerts");
    });
    app.router.on("route:showAlertsDetail",function(alertId, jobIds){
        var alert;
        if(_.isEqual(alertId,"create")){
            alert = new Alert.Model({jobIds: jobIds});
        }else{
            alert = app.alertCollection.get(alertId);
        }
        var view = new Alerts.DetailView({
            model:alert
        });
        app.showView(view,"#alert");
        app.makeHtmlTitle("Alert::"+name);
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
    app.on('loadJobTable',function(){
        var state = app.getCookie("spawn");
        if(!_.isUndefined(state) && state.jobCompact){
            app.trigger("loadJobCompactTable");
        }
        else{
            app.trigger("loadJobComftTable");
        }
    });
    app.on('loadJobCompactTable',function(){
        if(_.isUndefined(app.jobTable) || !_.isEqual(app.jobTable.id,'compactJobTable')){
            var state = app.getCookie("spawn") || {};
            state.jobCompact=true;
            app.setCookie("spawn",state);
            app.jobTable = new Jobs.CompactTable({
                id:"compactJobTable",
                collection:app.jobCollection
            });
        }
    });
    app.on('loadJobComftTable',function(){
        if(_.isUndefined(app.jobTable) || !_.isEqual(app.jobTable.id,'comfyJobTable')){
            var state = app.getCookie("spawn") || {};
            state.jobCompact=false;
            app.setCookie("spawn",state);
            app.jobTable= new Jobs.ComfyTableView({
                id:"comfyJobTable",
                collection:app.jobCollection
            });
        }
    });
    app.on("loadCommit",function(jobId,commit){
        app.trigger("loadJob",jobId);
        var commit = new Jobs.HistoryModel({
            jobUuid:jobId,
            commit:commit
        }).load().done(function(data){
            app.configModel.set("config",data);
        }).fail(function(data){
            Alertify.dialog.error("Error loading commit "+commit);
        });
    });
    app.on("loadJob",function(jobId){
        if(_.isEqual(jobId,"create")){
            if(_.isUndefined(app.job) || !app.job.isNew()){
                app.job = new Jobs.Model();
                app.job.unset("id");
                app.configModel = new Jobs.ConfigModel();
                app.parameterCollection = new Jobs.ParameterCollection();
            }
            app.makeHtmlTitle("New::Job");
        }
        else{
            if(_.isUndefined(app.job) || !_.isEqual(app.job.id,jobId) || !_.isEmpty(app.cloneId)){
                var job = app.jobCollection.get(jobId);
                if(_.isUndefined(job)){
                    Alertify.log.error("Job "+jobId+" not found.");
                    app.router.navigate("#jobs",{trigger:true});
                    return;
                }
                app.job=job;
                app.job.isClone=false;
                app.job.cloneId="";
                app.job.fetch();
            }
            if(_.isUndefined(app.configModel) || !_.isEqual(app.configModel.get("jobUuid"),jobId)){
                app.configModel = new Jobs.ConfigModel({
                    jobUuid:jobId
                });
                app.configModel.fetch();
            }
            if(_.isUndefined(app.parameterCollection) || !_.isEqual(app.parameterCollection.jobUuid,jobId)){
                app.parameterCollection = new Jobs.ParameterCollection();
                app.parameterCollection.jobUuid=jobId;
                app.parameterCollection.fetch();
            }
            app.makeHtmlTitle("Job::"+jobId);
        }
    });
    app.on("cloneJob",function(jobId){
        if(_.isUndefined(app.job) || !_.isEqual(app.job.cloneId,jobId)){
            var data= app.jobCollection.get(jobId).pick([
                "backups",
                'command',
                'creator',
                'dailyBackups',
                'description',
                'dontAutoBalanceMe',
                'hourlyBackups',
                'maxRunTime',
                'maxSimulRunning',
                'minionType',
                'monthlyBackups',
                'nodes',
                'onComplete',
                'onError',
                'ondone',
                'onerror',
                'owner',
                'parameters',
                'priority',
                'qc_canQuery',
                'qc_consecutiveFailureThreshold',
                'qc_queryTraceLevel',
                'queryConfig',
                'readOnlyReplicas',
                'rekickTimeout',
                'replicas',
                'replicationFactor',
                'weeklyBackups'
            ]);
            data.description = "CLONE "+data.description;
            app.job = new Jobs.Model(data);
            //app.job = app.jobCollection.get(jobId).clone();
            app.job.cloneId=jobId;
        }
        if(_.isUndefined(app.configModel) || !_.isEqual(app.configModel.get("jobUuid"),jobId)){
            app.configModel = new Jobs.ConfigModel({
                jobUuid:jobId
            });
            app.configModel.fetch();
        }
        if(_.isUndefined(app.parameterCollection) || !_.isEqual(app.parameterCollection.jobUuid,jobId)){
            app.parameterCollection = new Jobs.ParameterCollection();
            app.parameterCollection.jobUuid=jobId;
            app.parameterCollection.fetch();
        }
        app.makeHtmlTitle("Clone::"+jobId);
    });
    app.authenticate();
    domReady(function(){
        Backbone.history.start();
        $("#quiesceLink").click(function(event){
            event.stopImmediatePropagation();
            event.preventDefault();
            app.quiesce();
        });
        app.isQuiesced = _.isEqual(setupData.quiesced,"1");
        app.checkQuiesced();
        new Jobs.InfoMetricView({
            el:"div#infoMetricBox",
            model:app.jobInfoMetricModel
        }).render();
    });
    app.server.on("cluster.quiesce",function(message){
        app.isQuiesced = Boolean(message.quiesced);
        if(app.isQuiesced){
            Alertify.log.info("Cluster has been quiesced by "+message.username);
        }
        else{
            Alertify.log.info("Cluster has been reactivatd by "+message.username);
        }
        app.checkQuiesced();
    });
    window.app=app;
    window.DataTable=DataTable;
});
