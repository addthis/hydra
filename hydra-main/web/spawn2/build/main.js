/******/ (function(modules) { // webpackBootstrap
/******/ 	// install a JSONP callback for chunk loading
/******/ 	var parentJsonpFunction = window["webpackJsonp"];
/******/ 	window["webpackJsonp"] = function webpackJsonpCallback(chunkIds, moreModules) {
/******/ 		// add "moreModules" to the modules object,
/******/ 		// then flag all "chunkIds" as loaded and fire callback
/******/ 		var moduleId, chunkId, i = 0, callbacks = [];
/******/ 		for(;i < chunkIds.length; i++) {
/******/ 			chunkId = chunkIds[i];
/******/ 			if(installedChunks[chunkId])
/******/ 				callbacks.push.apply(callbacks, installedChunks[chunkId]);
/******/ 			installedChunks[chunkId] = 0;
/******/ 		}
/******/ 		for(moduleId in moreModules) {
/******/ 			modules[moduleId] = moreModules[moduleId];
/******/ 		}
/******/ 		if(parentJsonpFunction) parentJsonpFunction(chunkIds, moreModules);
/******/ 		while(callbacks.length)
/******/ 			callbacks.shift().call(null, __webpack_require__);
/******/
/******/ 	};
/******/
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// object to store loaded and loading chunks
/******/ 	// "0" means "already loaded"
/******/ 	// Array means "loading", array contains callbacks
/******/ 	var installedChunks = {
/******/ 		0:0
/******/ 	};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId])
/******/ 			return installedModules[moduleId].exports;
/******/
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			exports: {},
/******/ 			id: moduleId,
/******/ 			loaded: false
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.loaded = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/ 	// This file contains only the entry chunk.
/******/ 	// The chunk loading function for additional chunks
/******/ 	__webpack_require__.e = function requireEnsure(chunkId, callback) {
/******/ 		// "0" is the signal for "already loaded"
/******/ 		if(installedChunks[chunkId] === 0)
/******/ 			return callback.call(null, __webpack_require__);
/******/
/******/ 		// an array means "currently loading".
/******/ 		if(installedChunks[chunkId] !== undefined) {
/******/ 			installedChunks[chunkId].push(callback);
/******/ 		} else {
/******/ 			// start chunk loading
/******/ 			installedChunks[chunkId] = [callback];
/******/ 			var head = document.getElementsByTagName('head')[0];
/******/ 			var script = document.createElement('script');
/******/ 			script.type = 'text/javascript';
/******/ 			script.charset = 'utf-8';
/******/ 			script.async = true;
/******/
/******/ 			script.src = __webpack_require__.p + "" + chunkId + ".main.js";
/******/ 			head.appendChild(script);
/******/ 		}
/******/ 	};
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "/spawn2/build/";
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(0);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/*!************************!*\
  !*** ./src/js/main.js ***!
  \************************/
/***/ function(module, exports, __webpack_require__) {

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
	
	__webpack_require__.e/* require */(1, function(__webpack_require__) { var __WEBPACK_AMD_REQUIRE_ARRAY__ = [
	    __webpack_require__(/*! oboe */ 1),
	    __webpack_require__(/*! app */ 2),
	    __webpack_require__(/*! alertify */ 7),
	    __webpack_require__(/*! jscookie */ 9),
	    __webpack_require__(/*! router */ 3),
	    __webpack_require__(/*! jobs */ 10),
	    __webpack_require__(/*! macro */ 56),
	    __webpack_require__(/*! alias */ 61),
	    __webpack_require__(/*! alerts */ 65),
	    __webpack_require__(/*! command */ 69),
	    __webpack_require__(/*! host */ 73),
	    __webpack_require__(/*! layout.views */ 37),
	    __webpack_require__(/*! task */ 30),
	    __webpack_require__(/*! task.log */ 77),
	    __webpack_require__(/*! graph */ 35),
	    __webpack_require__(/*! git */ 78),
	    __webpack_require__(/*! alerts */ 65),
	    __webpack_require__(/*! settings */ 80),
	    __webpack_require__(/*! datatable */ 12),
	    __webpack_require__(/*! domReady */ 82),
	    __webpack_require__(/*! backbone */ 4),
	    __webpack_require__(/*! underscore */ 5),
	    __webpack_require__(/*! jquery */ 6),
	    __webpack_require__(/*! bootstrap */ 83),
	    __webpack_require__(/*! date */ 96)
	]; (function(
	    oboe,
	    app,
	    alertify,
	    Cookies,
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
	    domReady,
	    Backbone,
	    _,
	    $
	){
	    alertify.defaults.glossary.title="";
	    alertify.defaults.transition = "slide";
	    alertify.defaults.theme.ok = "btn btn-primary";
	    alertify.defaults.theme.cancel = "btn btn-danger";
	    alertify.defaults.theme.input = "form-control";
	    alertify.minimalDialog || alertify.dialog('minimalDialog',function(){
	        return {
	            main:function(content){
	                this.setContent(content);
	            }
	        };
	    });
	    $('#loginForm').on('submit', app.authenticate);
	    $('#loginButton').on('click', app.login);
	    $('#sudoCheckbox').on('click', app.sudo);
	    $('#logoutButton').on('click', app.logout);
	
	    app.jobCollection = new Jobs.Collection([]);
	    app.hostCollection = new Host.Collection([]);
	    app.alertCollection = new Alert.Collection([]);
	    app.commandCollection = new Command.Collection([]);
	    app.macroCollection = new Macro.Collection([]);
	    app.aliasCollection = new Alias.Collection([]);
	
	    oboe({url: '/update/setup'})
	        .node('quiesce', function (quiesce) {
	            app.isQuiesced = quiesce;
	            app.checkQuiesced();
	        })
	        .node('queryHost', function (queryHost) {
	            app.queryHost = queryHost;
	        })
	        .node('jobs[*]', function (job) {
	            app.jobCollection.add(job);
	        })
	        .node('hosts[*]', function (host) {
	            app.hostCollection.add(host);
	        })
	        .node('alerts[*]', function (alert) {
	            app.alertCollection.add(alert);
	        })
	        .node('commands[*]', function (command) {
	            app.commandCollection.add(command);
	        })
	        .node('macros[*]', function (macro) {
	            app.macroCollection.add(macro);
	        })
	        .node('aliases[*]', function (alias) {
	            app.aliasCollection.add(alias);
	        });
	
	    app.server.connect();
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
	                alertify.error("Error loading commit: "+xhr.responseText);
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
	                alertify.error("Error loading diff: "+xhr.responseText);
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
	    app.router.on("route:showChangePermissions",function(jobIds){
	        new Jobs.ChangePermissionsModalView({jobIds:jobIds}).render();
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
	        var table = new Alerts.TableView({
	            id:"alertTable",
	            collection:app.alertCollection
	        });
	        app.showView(table,"#alerts");
	        app.makeHtmlTitle("Alerts");
	    });
	    app.router.on("route:showAlertsTableFiltered",function(jobIdFilter) {
	    	app.router.navigate("#alerts", {trigger: true});    	    	
			// Modify the table filter and apply it to the alert list.
			var inp = $("#alertTable_filter").find("input");
			inp.val(jobIdFilter);
			var event = $.Event("keypress");
			event.which = 13;
			inp.trigger(event);    	
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
	        app.showView(view,"#alerts");
	        app.makeHtmlTitle("Alert::"+name);
	    });    
	    app.user.on("change:username",function(){
	        $("#usernameBox").html(app.user.get("username"));
	    });
	    app.on('loadJobTable',function(){
	        var state = Cookies.getJSON("spawn");
	        if(!_.isUndefined(state) && state.jobCompact){
	            app.trigger("loadJobCompactTable");
	        }
	        else{
	            app.trigger("loadJobComftTable");
	        }
	    });
	    app.on('loadJobCompactTable',function(){
	        if(_.isUndefined(app.jobTable) || !_.isEqual(app.jobTable.id,'compactJobTable')){
	            var state = Cookies.getJSON("spawn") || {};
	            state.jobCompact=true;
	            Cookies.set("spawn", state);
	            app.jobTable = new Jobs.CompactTable({
	                id:"compactJobTable",
	                collection:app.jobCollection
	            });
	        }
	    });
	    app.on('loadJobComftTable',function(){
	        if(_.isUndefined(app.jobTable) || !_.isEqual(app.jobTable.id,'comfyJobTable')){
	            var state = Cookies.getJSON("spawn") || {};
	            state.jobCompact=false;
	            Cookies.set("spawn", state);
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
	            alertify.error("Error loading commit "+commit);
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
	                    alertify.error("Job "+jobId+" not found.");
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
	                'dailyBackups',
	                'description',
	                'dontCloneMe',
	                'dontAutoBalanceMe',
	                'hourlyBackups',
	                'maxRunTime',
	                'maxSimulRunning',
	                'minionType',
	                'monthlyBackups',
	                'nodes',
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
	                'weeklyBackups',
	                'autoRetry'
	            ]);
	            data.description = "CLONE "+data.description;
	            app.job = new Jobs.Model(data);
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
	    app.initialize();
	    domReady(function(){
	        Backbone.history.start();
	        $("#healthCheckLink").click(function(event){
	            event.stopImmediatePropagation();
	            event.preventDefault();
	            app.healthCheck();
	            $(event.target).parents(".open").find("[data-toggle=dropdown]").dropdown("toggle");
	        });
	        $("#quiesceLink").click(function(event){
	            event.stopImmediatePropagation();
	            event.preventDefault();
	            app.quiesce();
	        });
	        
	        new Jobs.InfoMetricView({
	            el:"div#infoMetricBox",
	            model:app.jobInfoMetricModel
	        }).render();
	    });
	    app.server.on("cluster.quiesce",function(message){
	        app.isQuiesced = Boolean(message.quiesced);
	        if(app.isQuiesced){
	            alertify.message("Cluster has been quiesced by "+message.username);
	        }
	        else{
	            alertify.message("Cluster has been reactivatd by "+message.username);
	        }
	        app.checkQuiesced();
	    });
	    window.app=app;
	    window.DataTable=DataTable;
	}.apply(null, __WEBPACK_AMD_REQUIRE_ARRAY__));});


/***/ }
/******/ ]);
//# sourceMappingURL=main.js.map