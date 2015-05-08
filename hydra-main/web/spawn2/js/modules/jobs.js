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
define([
    "app",
    "alertify",
    "jscookie",
    "modules/datatable",
    "modules/util",
    "modules/editor",
    "modules/task",
    "modules/graph",
    "modules/layout.views",
    "text!../../templates/job.filter.html",
    "text!../../templates/job.selectable.html",
    "text!../../templates/task.divider.html",
    "text!../../templates/task.detail.divider.html",
    "text!../../templates/job.configuration.html",
    "text!../../templates/job.setting.html",
    "text!../../templates/job.detail.html",
    "text!../../templates/job.alerts.html",
    "text!../../templates/job.dependencies.html",
    "text!../../templates/job.expandedconf.html",
    "text!../../templates/job.history.html",
    "text!../../templates/job.task.html",
    "text!../../templates/job.parameter.html",
    "text!../../templates/job.taskdetail.html",
    "text!../../templates/job.task.breadcrumbs.html",
    "text!../../templates/job.revert.modal.html",
    "text!../../templates/job.permissions.modal.html",
    "text!../../templates/job.table.info.html",
    "text!../../templates/job.checkdirs.html"
],
function(
    app,
    alertify,
    Cookies,
    DataTable,
    util,
    Editor,
    Task,
    Graph,
    Layout,
    jobFilterTemplate,
    jobSelectableTemplate,
    taskDividerTemplate,
    taskDetailDividerTemplate,
    jobConfigurationTemplate,
    jobSettingTemplate,
    jobDetailTemplate,
    jobAlertsTemplate,
    jobDependenciesTemplate,
    jobExpandedConfTemplate,
    jobHistoryTemplate,
    jobTaskTableTemplate,
    jobParameterTemplate,
    jobTaskDetailTemplate,
    jobTaskBreadcrumbTemplate,
    jobRevertModalTemplate,
    jobPermissionsModalTemplate,
    jobTableInfoTemplate,
    jobCheckDirsTemplate
){
    var showStartStopStateChange = function(data, state){
        if (data.success.length > 0) {
            alertify.success(data.success.length + " job(s) have been " + state, 5);
        }
        if (data.error.length > 0) {
            alertify.error(data.error.length + " job(s) have not been " + state, 5);
        }
        if (data.unauthorized.length > 0) {
            alertify.error(data.unauthorized.length + " job(s) insufficient privileges");
        }
    };
    var States = [
        "IDLE",
        "SCHEDULED",
        "RUNNING",
        "DEGRADED",
        "UNKNOWN",
        "ERROR",
        "REBALANCE"
    ];
    var StateLabels = [
        "label-default",
        "label-info",
        "label-success",
        "label-inverse",
        "label-inverse",
        "label-danger",
        "label-info"
    ];
    var Model = Backbone.Model.extend({
        idAttribute:"id",
        url:function(){
            return "/job/get?id="+this.id;
        },
        initialize:function(options){
            options = options || {};
            this.cloneId = (_.has(options,"cloneId")?options.cloneId:"");
        },
        parse:function(data){
            data.DT_RowId=data.id;
            data.DT_RowClass='job-row';
            data.submitTime = data.submitTime || "";
            data.endTime = data.endTime || "";
            data.creator = data.creator || "";
            data.owner = data.owner || "";
            data.group = data.group || "";
            data.state = (_.has(data,'state')?data.state:4);
            if(data.disabled){
                data.status = "disabled";
            }
            else if(data.stopped){
                data.status = "stopped";
            }
            else{
                data.status="enabled";
            }
            data.bytes = data.bytes || "";
            data.maxRunTime = data.maxRunTime || "";
            data.rekickTimeout = data.rekickTimeout || "";
            data.nodes = (_.isArray(data.nodes)?data.nodes.length: data.nodes);
            if(_.has(data,"config")){
                delete data.config;
            }
            data.stateText = States[data.state];
            data.stateLabel = StateLabels[data.state];
            data.parameters = data.parameters || [];
            data.onComplete=data.onComplete || "";
            data.onError=data.onError || "";
            if(!_.isEmpty(data.queryConfig)){
                data.qc_canQuery=data.queryConfig.canQuery;
            }
            else{
                data.qc_canQuery=false;
            }
            data.queryConfig=undefined;
            return data;
        },
        defaults: function() {
            return {
                description:"(no title)",
                state:4,
                creator: app.user.get("username"),
                owner: app.user.get("username"),
                submitTime:-1,
                endTime:-1,
                status:"",
                maxRunTime:"",
                rekickTimeout:"",
                nodes:"",
                bytes:"",
                parameters:[],
                alerts:[],
                command:'default-task',
                nodes:1,
                stateText:"",
                stateLabel:"",
                minionType:"default"
            }
        },
        rebalance:function(){
            var self=this;
            var parameters = {}
            parameters["id"] = self.id;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/rebalance",
                type: "GET",
                data: parameters,
                statusCode: {
                    500: function(data) {
                        alertify.error(e.responseText,5);
                    },
                    200: function(data){
                        alertify.success(data.responseText,2);
                    }
                },
                dataType: "json"
            });
        },
        enable : function(unsafe) {
            var self = this;
            var parameters = {}
            parameters["jobs"] = self.id;
            parameters["enable"] = 1;
            parameters["unsafe"] = unsafe;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/enable",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                self.showEnableStateChange(data, "enabled", unsafe);
            }).fail(function(e){
                alertify.error("Error enabling job "+self.id+"<br/>" + e.responseText);
            });
        },
        disable : function() {
            var self = this;
            var parameters = {}
            parameters["jobs"] = self.id;
            parameters["enable"] = 0;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/enable",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                self.showEnableStateChange(data, "disabled");
            }).fail(function(e){
                alertify.error("Error disabling job "+self.id+"<br/>" + e.responseText);
            });
        },
        showEnableStateChange:function(data, state, unsafe){
            var jobId = this.id;
            if (data.changed.length > 0) {
                var v = (unsafe ? "unsafely " : "") + state;
                alertify.success("Job " + jobId + " has been " + v, 2);
            } else if (data.unchanged.length > 0) {
                alertify.message("Job " + jobId + " is already " + state, 2);
            } else if (data.notFound.length > 0) {
                alertify.error("Job " + jobId + " is not found");
            } else if (data.notAllowed.length > 0) {
                alertify.error("Job " + jobId + " must be IDLE to be enabled safely");
            } else if (data.notPermitted.length > 0) {
                alertify.error("User has insufficient privileges for Job " + jobId);
            } else {
                alertify.error("Unexpected response data. Check console log")
                console.log("Unexpected response data from /job/enable call: " + data);
            }
        },
        revert:function(params){
            var self = this;
            var data = _.extend(params,{
                id:self.get("id")
            });
            app.authQueryParameters(data);
            return $.ajax({
                url:"/job/revert",
                type: "GET",
                data:data
            });
        },
        fixDirs:function(node){
            var self=this;
            node = node || -1;
            var parameters = {}
            parameters["id"] = self.id;
            parameters["node"] = node;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/fixJobDirs",
                type: "GET",
                data: parameters,
                dataType:"text"
            }).done(function(data){
                alertify.success(data);
            }).fail(function(xhr){
                alertify.error(xhr.responseText);
            });
        },
        query : function(){
            window.open("http://"+app.queryHost+"/query/index.html?job="+this.id,"_blank");
        },
        delete : function(dontShowSuccessAlert){
            var self=this;
            var parameters = {}
            parameters["id"] = self.id;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/delete",
                type: "GET",
                data: parameters,
                statusCode: {
                    304: function() {
                        alertify.error("Job with id "+self.id+" has \"do not delete\" parameter enabled.");
                    },
                    404: function() {
                        alertify.error("Job with id "+self.id+" was not found.");
                    },
                    500: function(res){
                        alertify.error("Error deleting job "+self.id+":\n"+res.responseText);
                    },
                    200: function(){
                        if(!dontShowSuccessAlert){
                            app.router.navigate("#jobs",{trigger:true});
                            alertify.success("Job deleted successfully.");
                        }
                    }
                },
                dataType: "text"
            });
        },
        kick : function(){
            var self = this;
            var parameters = {}
            parameters["jobid"] = self.id;
            parameters["priority"] = 1;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/start",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                showStartStopStateChange(data, "started");
            }).fail(function(e){
                alertify.error("Error kicking: "+self.id+". <br/> "+e.responseText);
            });
        },
        stop : function(){
            var self=this;
            var parameters = {}
            parameters["id"] = self.id;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/stop",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                showStartStopStateChange(data, "stopped");
            }).fail(function(e){
                alertify.error("Error stopping: "+self.id+". <br/> "+e.responseText);
            });
        },
        kill : function(){
            var self=this;
            var parameters = {}
            parameters["id"] = self.id;
            parameters["force"] = "true";
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/stop",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                alertify.message(self.id+" job killed.",2)
            }).fail(function(e){
                alertify.error("Error killing: "+self.id+". <br/> "+e.responseText);
            });
        },
        save : function(param){
            var self=this;
            var data = _.extend(_.omit(this.toJSON(),'parameters','alerts','config','DT_RowId','DT_RowClass'),param);
            data= _.omit(data,'state');
            data.command=$("#command").val();
            if (!_.isEmpty(this.commit)) {
                data.commit=this.commit;
            }
            var parameters = {}
            if (!this.isNew()) {
                parameters['id'] = self.id;
            }
            app.authQueryParameters(parameters);
            return $.ajax({
                url: "/job/save?" + $.param(parameters),
                data: data,
                type: "POST"
            });
        },
        validate:function(config,params){
            var data={
                id:this.id,
                config:config
            };
            _.each(params,function(param){
                data["sp_"+param.name]=param.value;
            });
            return $.ajax({
                url: "/job/validate",
                type: "POST",
                data: data,
                dataType:"json"
            });
        }
    });
    var ParameterModel = Backbone.Model.extend({
        idAttribute:"name",
        defaults:{
            defaultValue:"",
            name:"",
            value:""
        }
    });
    var ParameterCollection = Backbone.Collection.extend({
        initialize:function(options){
        },
        url:function(){
            return "/job/get?id="+this.jobUuid+"&field=parameters";
        },
        sync: function(method, model, options){
            if(_.isEqual(method,'read')){
                var self=this, url=self.url();
                var ajax = $.ajax({
                    url:"/job/get",
                    data:{
                        id:self.jobUuid,
                        field:"parameters"
                    },
                    success:function(data){
                        var models=[];
                        _.each(data,function(param){
                            var model = new ParameterModel(param);
                            models.push(model);
                        });
                        self.reset(models);
                    },
                    dataType:"json"
                });
                return ajax;
            }
            else{
                return Backbone.sync(method, model, options);
            }
        },
        model:ParameterModel
    });
    var ConfigModel = Backbone.Model.extend({
        idAttribute:"jobUuid",
        defaults:{
            config:"",
            savedConfig:""
        },
        initialize:function(options){
        },
        url:function(){
            return "/job/get?id="+this.get("jobUuid")+"&field=config";
        },
        sync: function(method, model, options){
            if(_.isEqual(method,'read')){
                var self=this,url=self.url();
                var ajax = $.ajax({
                    url:"/job/get",
                    data:{
                        id:self.get("jobUuid"),
                        field:"config"
                    },
                    type:"GET",
                    dataType:"text"
                }).done(function(data){
                    self.set("config",data);
                    self.set("savedConfig",data);
                }).fail(function(xhr){
                    alertify.error("Error loading config: "+xhr.responseText);
                });
                return ajax;
            }
            else{
                return Backbone.sync(method, model, options);
            }
        }
    });
    var HistoryModel = Backbone.Model.extend({
        idAttribute:"commit",
        defaults:{
            commit:"",
            time:"",
            msg:""
        },
        diff:function(){
            var self=this;
            var data = {
                id: self.get("jobUuid"),
                commit: self.get("commit")
            };
            return $.ajax({
                url: "/job/config.diff",
                type: "GET",
                data: data,
                dataType:"text"
            });
        },
        load: function(){
            var self=this;
            var data = {
                id: self.get("jobUuid"),
                commit: self.get("commit")
            };
            return $.ajax({
                url: "/job/config.view",
                type: "GET",
                data: data,
                dataType: "text"
            });
        }
    });
    var HistoryCollection = Backbone.Collection.extend({
        initialize:function(options){
            //this.jobUuid=options.jobUuid;
        },
        sync: function(method, model, options){
            if(_.isEqual(method,'read')){
                var self=this;
                var ajax=$.ajax({
                    url: "/job/history",
                    type: "GET",
                    data: {
                        id:self.jobUuid
                    },
                    success: function(data){
                        var models=[];
                        _.each(data,function(history){
                            var model = new HistoryModel(history);
                            model.set("jobUuid",self.jobUuid);
                            models.push(model);
                        });
                        self.reset(models);
                    },
                    fail: function(e){
                        throw new Error(e.error());
                    },
                    dataType: "json"
                })
                return ajax;
            }
            else{
                return Backbone.sync(method, model, options);
            }
        }
    });
    var ExpandedConfigModel = Backbone.Model.extend({
        defaults:{
            config:""
        },
        sync: function(method, model, options){
            if(_.isEqual(method,'read')){
                var self=this;
                var data = this.toJSON();
                var ajax = $.ajax({
                    url:"/job/expand",
                    data:data,
                    type:"POST"
                }).done(function(data){
                    self.set("expanded",data);
                }).fail(function(xhr){
                    alertify.error("Error expanding config: "+xhr.responseText);
                });
                return ajax;
            }
            else{
                return Backbone.sync(method, model, options);
            }
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/job/list",
        initialize:function(){
            _.bindAll(this,'handleJobUpdate');
            this.listenTo(app.server,'job.update',this.handleJobUpdate);
            this.listenTo(app.server,'job.delete',this.handleJobDelete);
        },
        parse:function(collection){
            var array = new Array(collection.length);
            _.each(collection,function(model,idx){
                array[idx]= Model.prototype.parse(model);
            });
            return array;
        },
        model:Model,
        handleJobUpdate:function(data){
            var job = this.get(data.id);
            if(!_.isUndefined(job)){
                job.set(
                    Model.prototype.parse(data)
                );
            }
            else{
                job=new Model(
                    Model.prototype.parse(data)
                );
                this.add([job],{merge:true});
            }
        },
        handleJobDelete:function(data){
            var job = this.get(data.id);
            if(!_.isUndefined(job)){
                this.remove([job]);
            }
        },
        kickSelected:function(jobIds){
            var self = this;
            var count = jobIds.length;
            var parameters = {}
            parameters["jobid"] = jobIds.join();
            parameters["priority"] = 1;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/start",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                showStartStopStateChange(data, "started");
            }).fail(function(e){
                alertify.error("Error kicking: "+count+" jobs. <br/> "+e.responseText);
            });
        },
        stopSelected:function(jobIds){
            var self = this;
            var count = jobIds.length;
            var parameters = {}
            parameters["jobid"] = jobIds.join();
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/stop",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                showStartStopStateChange(data, "stopped");
            }).fail(function(e){
                alertify.error("Error stopping: "+count+" jobs. <br/> "+e.responseText);
            });
        },
        killSelected:function(jobIds){
            var count = jobIds.length;
            var parameters = {}
            parameters["jobid"] = jobIds.join();
            parameters["force"] = "true";
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/stop",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                showStartStopStateChange(data, "killed");
            }).fail(function(e){
                alertify.error("Error killing: "+count+" jobs. <br/> "+e.responseText);
            });
        },
        enableBatch:function(jobIds, unsafe){
            var self=this;
            var count = jobIds.length;
            var parameters = {}
            parameters["jobs"] = jobIds.join();
            parameters["enable"] = 1;
            parameters["unsafe"] = unsafe;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/enable",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                self.showEnableStateChange(data, "enabled", unsafe);
            }).fail(function(e){
                alertify.error("Error enabling: "+count+" jobs. <br/> "+e.responseText);
            });
        },
        disableBatch:function(jobIds){
            var self=this;
            var count = jobIds.length;
            var parameters = {}
            parameters["jobs"] = jobIds.join();
            parameters["enable"] = 0;
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/enable",
                type: "GET",
                data: parameters,
                dataType: "json"
            }).done(function(data){
                self.showEnableStateChange(data, "disabled");
            }).fail(function(e){
                alertify.error("Error disabling: "+count+" jobs. <br/> "+e.responseText);
            });
        },
        showEnableStateChange:function(data, state, unsafe){
            if (data.changed.length > 0) {
                var v = (unsafe ? "unsafely " : "") + state;
                alertify.success(data.changed.length + " job(s) have been " + v, 5);
            }
            if (data.unchanged.length > 0) {
                alertify.message(data.unchanged.length + " job(s) are already " + state, 5);
            }
            if (data.notFound.length > 0) {
                alertify.error(data.notFound.length + " job(s) are not found");
            }
            if (data.notAllowed.length > 0) {
                alertify.error(data.notAllowed.length + " job(s) cannot be enabled safely - they must be IDLE");
            }
            if (data.notPermitted.length > 0) {
                alertify.error(data.notPermitted.length + " job(s) insufficient privileges");
            }
        },
        deleteSelected:function(jobIds){
            var count = jobIds.length;
            var self=this;
            alertify.confirm("Are you sure you would like to DELETE " + count + " " + (count > 1 ? " jobs" : " job") + "?", function (resp) {

                _.each(jobIds,function(jobId){
                    var job = self.get(jobId);
                    if(!_.isUndefined(job)){
                        job.delete(true);
                    }
                });
            });
        }
    });
    var InfoMetricView = Backbone.View.extend({
        initialize:function(options){
            this.el=options.el;
            this.listenTo(this.model,"change",this.render);
            this.listenTo(this.model,"reset",this.render);
        },
        template: _.template(jobTableInfoTemplate),
        render:function(){
            this.$el=$(this.el);
            var html = this.template(this.model.toJSON());
            this.$el.html(html);
            return this;
        }
    })
    var InfoMetricModel = Backbone.Model.extend({
        initialize:function(options){
            _.bindAll(this,'handleJobAdd','handleJobRemove','handleJobNodesChange','handleTaskQueueChange','handleJobReset','handleRunningChange','handleDoneChange');
            this.listenTo(app.server,"task.queue.size",this.handleTaskQueueChange);
            this.listenTo(app.hostCollection,"change:diskUsed",this.handleDiskUsedChange);
            this.listenTo(app.hostCollection,"change:diskMax",this.handleDiskMaxChange);
            this.listenTo(app.hostCollection,"change:availableTaskSlots",this.handleAvailTaskChange);
            this.listenTo(app.jobCollection,"add",this.handleJobAdd);
            this.listenTo(app.jobCollection,"reset",this.handleJobReset);
            this.listenTo(app.jobCollection,"change:nodes",this.handleJobNodesChange);
            this.listenTo(app.jobCollection,"change:running",this.handleRunningChange);
            this.listenTo(app.jobCollection,"change:errored",this.handleErroredChange);
            this.listenTo(app.jobCollection,"change:done",this.handleDoneChange);
            this.listenTo(app.jobCollection,"remove",this.handleJobRemove);
            this.listenTo(app.hostCollection,"reset",this.handleHostReset);
            this.handleJobReset();
            this.handleHostReset();
        },
        defaults:{
            tasksCount:0,
            queuedCount:0,
            queuedCountNoSlot:0,
            erroredCount:0,
            queuedErrorCount:0,
            runningCount:0,
            jobCount:0,
            availTaskSlots:0,
            disk:0,
            diskUsed:0,
            diskMax:0,
            hostCount:0
        },
        handleHostReset:function(){
            var diskUsed= 0,diskMax= 0, avail=0;
            app.hostCollection.each(function(hostModel){
                diskUsed+=hostModel.get("diskUsed");
                diskMax+=hostModel.get("diskMax");
                if (!hostModel.get("dead")) {
                	avail+=hostModel.get("availableTaskSlots");
                }
            });
            if(diskMax>0){
                var disk = Math.floor((diskUsed/diskMax)*100)/100;
                this.set("disk",disk);
            }
            this.set("diskUsed",diskUsed);
            this.set("diskMax",diskMax);
            this.set("availTaskSlots",avail);
            this.set("hostCount",app.hostCollection.length);
        },
        handleDiskChange:function(){
            if(this.get("diskMax")>0){
                var diskUsed=this.get("diskUsed"), diskMax=this.get("diskMax");
                var disk = Math.floor((diskUsed*100)/diskMax)/100.0;
                this.get("disk",disk);
            }
        },
        handleDiskUsedChange:function(model){
            var delta = parseInt(model.get("diskUsed"))-parseInt(model.previous("diskUsed"));
            if(_.isNumber(delta) && !_.isNaN(delta)){
                var prev = this.get("diskUsed");
                this.set("diskUsed",prev+delta);
            }
            this.handleDiskChange();
        },
        handleDiskMaxChange:function(model){
            var delta = parseInt(model.get("diskMax"))-parseInt(model.previous("diskMax"));
            if(_.isNumber(delta) && !_.isNaN(delta)){
                var prev = this.get("diskMax");
                this.set("diskMax",prev+delta);
            }
            this.handleDiskChange();
        },
        handleAvailTaskChange:function(model){
            var delta = parseInt(model.get("availableTaskSlots"))-parseInt(model.previous("availableTaskSlots"));
            if(_.isNumber(delta) && !_.isNaN(delta) && !this.get("dead")){
                var prev = this.get("availTaskSlots");
                this.set("availTaskSlots",prev+delta);
            }
        },
        handleJobAdd:function(model){
            var tasks = parseInt(model.get("nodes"));
            if(_.isNumber(tasks) && !_.isNaN(tasks)){
                var prev = this.get("tasksCount");
                this.set("tasksCount",prev+tasks);
            }
            this.set("jobCount",this.get("jobCount")+1);
        },
        handleJobNodesChange:function(model){
            var delta = parseInt(model.get("nodes"))-parseInt(model.previous("nodes"));
            if(_.isNumber(delta) && !_.isNaN(delta)){
                var prev = this.get("tasksCount");
                this.set("tasksCount",prev+delta);
            }
        },
        handleErroredChange:function(model){
            var delta = parseInt(model.get("errored"))-parseInt(model.previous("errored"));
            if(_.isNumber(delta) && !_.isNaN(delta)){
                var prev = this.get("erroredCount");
                this.set("erroredCount",prev+delta);
            }
        },
        handleRunningChange:function(model){
            var delta = parseInt(model.get("running"))-parseInt(model.previous("running"));
            if(_.isNumber(delta) && !_.isNaN(delta)){
                var prev = this.get("runningCount");
                this.set("runningCount",prev+delta);
            }
        },
        handleDoneChange:function(model){
            var delta = parseInt(model.get("done"))-parseInt(model.previous("done"));
            if(_.isNumber(delta) && !_.isNaN(delta)){
                var prev = this.get("runningCount");
                this.set("runningCount",prev-delta);
            }
        },
        handleJobRemove:function(model){
            var tasks = parseInt(model.get("nodes"));
            if(_.isNumber(tasks) && !_.isNaN(tasks)){
                var prev = this.get("tasksCount");
                this.set("tasksCount",prev-tasks);
            }
            this.set("jobCount",this.get("jobCount")-1);
        },
        handleTaskQueueChange:function(data){
            this.set("queuedCount",data.size - data.sizeErr);
            this.set("queuedCountNoSlot",data.sizeSlot);
            this.set("queuedErrorCount",data.sizeErr);
        },
        handleJobReset:function(){
            var tasks= 0,running= 0,errored=0;
            app.jobCollection.each(function(model){
                tasks+=model.get("nodes");
                running+=model.get("running")-model.get("done");
                errored+=model.get("errored");
            });
            this.set("tasksCount",tasks);
            this.set("runningCount",running);
            this.set("erroredCount",errored);
            this.set("jobCount",app.jobCollection.length);
        }
    });
    var JobTable = DataTable.View.extend({
        initialize:function(options){
            _.bindAll(this,
                'render',
                'handleKickButtonClick',
                'handleStopButtonClick',
                'handleKillButtonClick',
                'handleEnableButtonClick',
                'handleDisableButtonClick',
                'handleDeleteButtonClick',
                'handleCreateAlertButtonClick',
                'handleChangePermissionsButtonClick',
                'handleFindDeletedJobButtonClick'
            );
            this.hasRendered=false;
            this.listenTo(app.user,"change:username",this.handleUsernameChange);
            DataTable.View.prototype.initialize.apply(this,[options]);
        },
        render:function(){
            DataTable.View.prototype.render.apply(this,[]);
            if(!this.hasRendered){
                this.views.selectable.find("#kickButton").on("click",this.handleKickButtonClick);
                this.views.selectable.find("#stopButton").on("click",this.handleStopButtonClick);
                this.views.selectable.find("#killButton").on("click",this.handleKillButtonClick);
                this.views.selectable.find("#enableButton").on("click",this.handleEnableButtonClick);
                this.views.selectable.find("#disableButton").on("click",this.handleDisableButtonClick);
                this.views.selectable.find("#deleteButton").on("click",this.handleDeleteButtonClick);
                this.views.selectable.find("#createAlertButton").on("click", this.handleCreateAlertButtonClick);
                this.views.selectable.find("#changePermissionsButton").on("click", this.handleChangePermissionsButtonClick);
                this.hasRendered=true;
            }
            // Find deleted job
            this.views.parent.find("#findDeletedJobButton").on("click", this.handleFindDeletedJobButtonClick);
            // Jobs filter
            var jobFilter = this.views.filter.find("#myJobs");
            jobFilter.data("value",app.user.get("username"));
            var state = this.views.table.fnSettings().oLoadedState;
            if(!_.isNull(state) && !_.isUndefined(state.aoSearchCols)){
                var col=state.aoSearchCols[jobFilter.data("index")];
                var search = col.sSearch;
                if(!_.isEmpty(search)){
                    jobFilter.addClass("selected");
                }
            }
            return this;
        },
        handleKickButtonClick:function(event){
            var ids = this.getSelectedIds(),self=this;
            if(app.isQuiesced){
                alertify.confirm("Cluster is quiesced, are you sure you want to kick "+ids.length+" job(s)?", function (e) {
                    self.collection.kickSelected(ids);
                });
            }else{
                self.collection.kickSelected(ids);
            }
        },
        handleStopButtonClick:function(event){
            var ids = this.getSelectedIds();
            this.collection.stopSelected(ids);
        },
        handleKillButtonClick:function(event){
            var ids = this.getSelectedIds();
            this.collection.killSelected(ids);
        },
        handleEnableButtonClick:function(event){
            var ids = this.getSelectedIds();
            // shift click triggers unsafe enable!
            this.collection.enableBatch(ids, event.shiftKey);
        },
        handleDisableButtonClick:function(event){
            var ids = this.getSelectedIds();
            this.collection.disableBatch(ids);
        },
        handleDeleteButtonClick:function(event){
            var ids = this.getSelectedIds();
            this.collection.deleteSelected(ids);
        },
        handleCreateAlertButtonClick:function(event){
            var ids = this.getSelectedIds();
            app.router.navigate("alerts/create/" + ids.join(), {trigger:true});
        },
        handleChangePermissionsButtonClick:function(event){
            var ids = this.getSelectedIds();
            app.router.trigger("route:showChangePermissions", ids);
        },
        handleFindDeletedJobButtonClick:function(event){
            alertify.prompt("Enter the deleted job ID:","",function(evt, str){
                window.open("/job/config.deleted?id="+str,"_blank");
            });
        },
        remove:function(){
            this.$el.detach();
        },
        handleUsernameChange:function(event){
            this.views.filter.find("#myJobs").data("value",app.user.get("username"));
            this.checkFilterState();
        }
    });
    var ComfyJobTable = JobTable.extend({
        initialize:function(options){
           // _.bindAll(this,'resize','handleReset','handleRemove','handleAdd','handleChange','handleFilter');
            _.bindAll(this,'drawCallback');
            options = options || {};
            this.id = options.id || "comfyJobTable";
            this.jobInfoMetricModel = options.jobInfoMetricModel;
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "mData": "submitTime",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "mData": "endTime",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"",
                    "sClass":"job-cb",
                    "mData": null,
                    "bSearchable":false,
                    "bSortable":false,
                    "mRender":function(val,type,data){
                        if(self.selectedIds[data.id]){
                            return "<input checked class='row_selectable' type='checkbox'></input>";
                        }
                        else{
                            return "<input class='row_selectable' type='checkbox'></input>";
                        }
                    }
                },
                {
                    "sTitle":"ID",
                    "sClass":"job-id",
                    "mData": "id",
                    "sWidth":"6%",
                    "bSearchable":true,
                    "mRender":function(val,type,data){
                        return "<a class='bs-tooltip' href='#jobs/"+val+"/quick'>"+val+"</a>";
                    }
                },
                {
                    "sTitle":"Creator",
                    "sClass":"job-creator center",
                    "mData": "creator",
                    "sWidth":"7%",
                    "bSearchable":true
                },
                {
                    "mData": "stateText",
                    "bVisible":false,
                    "bSearchable":true
                },
                {
                    "sTitle":"State",
                    "sClass":"job-state center",
                    "mData": "state",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return "<div class='label "+StateLabels[val]+"'>"+States[val]+"</div>";
                    },
                    "aDataSort":[5],
                    "aTargets":[6]
                },
                {
                    "sTitle":"Status",
                    "sClass":"job-status center",
                    "mData": "status",
                    "sWidth":"6%",
                    "bSearchable":true
                },
                {
                    "sTitle":"Submitted",
                    "sClass":"job-submitTime center",
                    "mData": "submitTime",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDateTimeText(val,"MM/dd/yy HH:mm");
                    },
                    "aDataSort":[0],
                    "aTargets":[8]
                },
                {
                    "sTitle":"Ended",
                    "sClass":"job-endTime center",
                    "mData": "endTime",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDateTimeText(val,"MM/dd/yy HH:mm");
                    },
                    "aDataSort":[1],
                    "aTargets":[9]
                },
                {
                    "sTitle":"Description",
                    "sClass":"job-desc",
                    "mData": "description",
                    "sWidth":"31%",//3,
                    "bSearchable":true,
                    "mRender":function(val,type,data){
                        return "<a class='bs-tooltip' href='#jobs/"+encodeURIComponent(data.id)+"/conf' class='bs-tooltip' data-toggle='tooltip' data-placement='right' title='' data-original-title='"+val+"'>"+val+"</a>";
                    }
                },
                {
                    "sTitle":"maxT",
                    "sClass":"job-maxt center",
                    "mData": "maxRunTime",
                    "sWidth":"4%",
                    "bSearchable":false
                },
                {
                    "sTitle":"rekT",
                    "sClass":"job-rekt center",
                    "mData": "rekickTimeout",
                    "sWidth":"4%",
                    "bSearchable":false
                },
                {
                    "sTitle":"Nodes",
                    "sClass":"job-nodes center",
                    "mData": "nodes",
                    "sWidth":"7%",
                    "bSearchable":false
                },
                {//10
                    "mData": "bytes",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"Size",
                    "sClass":"job-size center",
                    "mData": "bytes",
                    "sWidth":"5%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDFH(val);
                    },
                    "aDataSort":[14],
                    "aTargets":[15]
                },
                {
                    "sTitle":"",
                    "sClass":"job-actions center",
                    "mData": null,
                    "sWidth":"4%",
                    "bSortable":false,
                    "bSearchable":false,
                    "sSortData":1,
                    "mRender":function(val,type,data){
                        var html = "";
                        if(data.qc_canQuery){
                            html+="<a data-id='"+data.id+"' class='btn btn-default btn-tiny btn-blue' href='http://"+app.queryHost+"/query/index.html?job="+data.id+"' target='_blank'>Q</a>";
                        }
                        return html;
                    }
                }
            ];
            JobTable.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate: _.template(jobFilterTemplate)({username:app.user.get('username')}),
                selectableTemplate:jobSelectableTemplate,
                heightBuffer:80,
                columnFilterIndex:2,
                jobInfoMetricModel:this.jobInfoMetricModel,
                drawCallback:this.drawCallback,
                id:this.id,
                changeAttrs:[
                    'submitTime',
                    'endTime',
                    'stateText',
                    'state',
                    'disabled',
                    'running',
                    'errored',
                    'done',
                    'bytes',
                    'description',
                    'maxRunTime',
                    'rekickTimeout',
                    'qc_canQuery'
                ]
            }]);
        },
        render:function(){
            JobTable.prototype.render.apply(this,[]);
            this.views.parent.find("a#comfortableTable").parent().addClass("active");
            return this;
        },
        drawCallback:function(oSettings){
            if(this.dirty){
                var info = $(oSettings.nTableWrapper).find("div.dataTables_footer div.summary_info");
                this.$el.find("tr td.job-id a.bs-tooltip,tr td.job-desc a.bs-tooltip").hover(function(event){
                    var text = $(event.currentTarget).text();
                    if(info.length===1){
                        info.html(text);
                    }
                },function(event){
                    info.html("");
                });
            }
        }
    });
    var CompactTable = JobTable.extend({
        initialize:function(options){
            options = options || {};
            this.id = options.id || "compactTable";
            this.jobInfoMetricModel=options.jobInfoMetricModel;
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "sTitle":"",
                    "sClass":"job-cb",
                    "sWidth":"3%",
                    "mData": null,
                    "bSearchable":false,
                    "bSortable":false,
                    "mRender":function(val,type,data){
                        if(self.selectedIds[data.id]){
                            return "<input checked class='row_selectable' type='checkbox'></input>";
                        }
                        else{
                            return "<input class='row_selectable' type='checkbox'></input>";
                        }
                    }
                },
                {
                    "sTitle":"ID",
                    "sClass":"job-id",
                    "mData": "id",
                    "sWidth":"6%",
                    "bSearchable":true,
                    "mRender":function(val){
                        return "<a href='#jobs/"+val+"/quick'>"+val+"</a>";
                    }
                },//0
                {
                    "sTitle":"Creator",
                    "sClass":"job-creator center",
                    "mData": "creator",
                    "bSearchable":true,
                    "sWidth":"7%"
                },//1
                {
                    "mData": "stateText",
                    "bVisible":false,
                    "bSearchable":true
                },//2
                {
                    "sTitle":"State",
                    "sClass":"job-state center",
                    "mData": "state",
                    "sWidth":"5%",
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        var html = "<span class='label "+StateLabels[val]+"'>"+States[val]+"</span>";
                        return html;
                    },
                    "aDataSort":[3],
                    "aTargets":[4]
                },//3
                {
                    "mData": "submitTime",
                    "bVisible":false,
                    "bSearchable":false
                },//4
                {
                    "sTitle":"Submitted",
                    "sClass":"job-submitTime center",
                    "mData": "submitTime",
                    "sWidth":"7%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDateTimeText(val,"MM/dd HH:mm");
                    },
                    "aDataSort":[5],
                    "aTargets":[6]
                },//5
                {
                    "mData": "endTime",
                    "bVisible":false,
                    "bSearchable":false
                },//6
                {
                    "sTitle":"Ended",
                    "sClass":"job-endTime center",
                    "mData": "endTime",
                    "sWidth":"7%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDateTimeText(val,"MM/dd HH:mm");
                    },
                    "aDataSort":[7],
                    "aTargets":[8]
                },//7
                {
                    "sTitle":"Description",
                    "sClass":"job-desc",
                    "mData": "description",
                    "sWidth":"32%",
                    "bSearchable":true,
                    "mRender":function(val,type,data){
                        return "<a href='#jobs/"+encodeURIComponent(data.id)+"/conf'>"+val+"</a>";
                    }
                },//8
                {
                    "sTitle":"mT",
                    "sClass":"job-maxt center",
                    "mData": "maxRunTime",
                    "sWidth":"4%",
                    "bSearchable":false
                },//9
                {
                    "sTitle":"rT",
                    "sClass":"job-rekt center",
                    "mData": "rekickTimeout",
                    "sWidth":"4%",
                    "bSearchable":false
                },//10
                {
                    "sTitle":"N",
                    "sClass":"job-nodes center",
                    "mData": "nodes",
                    "sWidth":"4%",
                    "bSearchable":false
                },//11
                {
                    "sTitle":"R",
                    "sClass":"job-running center",
                    "mData": "running",
                    "sWidth":"4%",
                    "bSearchable":false
                },//12
                {
                    "sTitle":"E",
                    "sClass":"job-errored center",
                    "mData": "errored",
                    "sWidth":"4%",
                    "bSearchable":false
                },//13
                {
                    "sTitle":"D",
                    "sClass":"job-done center",
                    "mData": "done",
                    "sWidth":"4%",
                    "bSearchable":false
                },//14
                {
                    "mData": "bytes",
                    "bVisible":false,
                    "bSearchable":false
                },//15
                {
                    "sTitle":"Size",
                    "sClass":"job-size center",
                    "mData": "bytes",
                    "sWidth":"5%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDFH(val);
                    },
                    "aDataSort":[16],
                    "aTargets":[17]
                },//16
                {
                    "sTitle":"",
                    "sClass":"job-actions center",
                    "mData": "id",
                    "sWidth":"4%",
                    "bSortable":false,
                    "bSearchable":false,
                    "sSortData":1,
                    "mRender":function(val,type,data){
                        var html = "";
                        if(data.qc_canQuery){
                            html+="<a data-id='"+val+"' class='btn btn-default btn-tiny btn-blue' href='http://"+app.queryHost+"/query/index.html?job="+data.id+"' target='_blank'>Q</a>";
                        }
                        return html;
                    }
                }//17
            ];
            JobTable.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate: _.template(jobFilterTemplate)({username:app.user.get('username')}),
                selectableTemplate:jobSelectableTemplate,
                heightBuffer:80,
                columnFilterIndex:2,
                jobInfoMetricModel:this.jobInfoMetricModel,
                id:this.id,
                changeAttrs:[
                    'submitTime',
                    'endTime',
                    'stateText',
                    'state',
                    'running',
                    'errored',
                    'done',
                    'bytes',
                    'description',
                    'maxRunTime',
                    'rekickTimeout'
                ]
            }]);
        },
        render:function(){
            JobTable.prototype.render.apply(this,[]);
            this.views.parent.find("a#compactTable").parent().addClass("active");
            return this;
        }
    });
    var TaskDividerView = Backbone.View.extend({
        className:"task-divider",
        initialize:function(){

        },
        template: _.template(taskDividerTemplate),
        render:function(){
            var html = this.template(this.model.toJSON());
            this.$el.html(html);
            return this;
        }
    });
    var TaskDetailDividerView = Backbone.View.extend({
        className:"task-divider",
        initialize:function(options){
            this.nodeNumber=options.nodeNumber;
        },
        template: _.template(taskDetailDividerTemplate),
        render:function(){
            var html = this.template({
                job:this.model.toJSON()
            });
            this.$el.html(html);
            return this;
        }
    });
    var BackupModel = Backbone.Model.extend({
        idAttribute:"jobUuid",
        url:function(){
            return "/job/backups.list?id="+this.get("jobUuid")+"&node="+this.get("node");
        },
        sync: function(method, model, options){
            if(_.isEqual(method,'read')){
                var self=this, url=self.url();
                var ajax = $.ajax({
                    url:url,
                    data:{
                        id:self.jobUuid,
                        node:self.get("node")
                    }
                }).done(function(data){
                    self.set(data);
                    self.trigger("change");
                });
                return ajax;
            }
        }
    });
    var ChangePermissionsModalView = Backbone.View.extend({
        className:"modal fade",
        template: _.template(jobPermissionsModalTemplate),
        events:{
            "click button#jobPermissionsModalSubmit":"handleSubmitButtonClick",
            "hidden.bs.modal":"close",
        },
        initialize: function(options){
            _.bindAll(this, 'handleSubmitButtonClick', 'close');
            this.jobIds = options.jobIds;
        },
        render: function(){
            var html = this.template();
            this.$el.html(html);
            this.$el.modal("show");
            return this;
        },
        handleSubmitButtonClick: function() {
            var parameters = {}
            parameters["jobs"] = this.jobIds.join();
            parameters["owner"] = $('input[name="chownModal"]').val();
            parameters["group"] = $('input[name="chgrpModal"]').val();
            parameters["ownerWritable"] = $('input[name="ownerWritable"]:checked').val();
            parameters["groupWritable"] = $('input[name="groupWritable"]:checked').val();
            parameters["worldWritable"] = $('input[name="worldWritable"]:checked').val();
            parameters["ownerExecutable"] = $('input[name="ownerExecutable"]:checked').val();
            parameters["groupExecutable"] = $('input[name="groupExecutable"]:checked').val();
            parameters["worldExecutable"] = $('input[name="worldExecutable"]:checked').val();
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/permissions",
                type: "POST",
                data: parameters,
                dataType: "json"
            }).done(function(data) {
               if (data.changed.length > 0) {
                    alertify.success(data.changed.length + " job(s) have been updated");
                }
                if (data.unchanged.length > 0) {
                    alertify.message(data.unchanged.length + " job(s) already had these changes");
                }
                if (data.notFound.length > 0) {
                    alertify.error(data.notFound.length + " job(s) are not found");
                }
                if (data.notPermitted.length > 0) {
                    alertify.error(data.notPermitted.length + " job(s) insufficient privileges");
                }
            }).fail(function(e){
                alertify.error("Error changing permissions" + e.responseText);
            });
        },
        close: function() {
            this.remove();
            this.unbind();
        }
    });
    var BackupModalView = Backbone.View.extend({
        className:"modal fade",
        template: _.template(jobRevertModalTemplate),
        events:{
            "click button#runButton":"handleRunButtonClick",
            "click button#goldButton":"handleGoldButtonClick",
            "click button#hourlyButton":"handleHourlyButtonClick",
            "click button#dailyButton":"handleDailyButtonClick",
            "click button#weeklyButton":"handleWeeklyButtonClick"
        },
        initialize:function(options){
            _.bindAll(this,
                'handleButtonClickForTimestampRevert',
                'handleButtonClickForRevisionRevert',
                'handleRunButtonClick',
                'handleGoldButtonClick',
                'handleDailyButtonClick',
                'handleHourlyButtonClick',
                'handleWeeklyButtonClick');
            this.backupModel = options.backupModel;
            this.listenTo(this.backupModel,"change",this.handleBackupChange);
        },
        render:function(){
            var node = this.backupModel.get("node");
            var html = this.template({
                model: this.model.toJSON(),
                title: node > -1 ? "Revert task " + node : "Revert job"
            });
            this.$el.html(html);
            this.$el.modal("show");
            this.showMessage("Searching for available backups...");
            return this;
        },
        handleBackupChange:function(){
            var self=this;
            var node = this.backupModel.get("node");
            var data = this.backupModel.toJSON()
            var count = 0;
            if (node == -1) {
                // reverting a job - don't populate/show gold backups by timestamp. see T51329
                var selects = ["hourly", "daily", "weekly", "monthly"]
            } else {
                var selects = ["gold", "hourly", "daily", "weekly", "monthly"]
            }
            _.each(selects,function(type){
                var select = self.$el.find("#"+type+"Select");
                var options = "";
                _.each(data[type],function(ts){
                    options+="<option value='"+ts+"'>"+util.convertToDateTimeText(ts)+"</option>"
                    count++;
                });
                select.html(options);
                if(!_.isEmpty(options)){
                    select.closest("tr").show();
                }
            });
            if (count == 0) {
                this.showMessage("There is no backup!");
            } else {
                this.showMessage("");
            }
            if (node >= 0) {
                // reverting a task - hide the revert by revision selection because it will be
                // replaced by the dropdown for timestamp based backups
                this.$el.find("#goldRevertByRevision").hide();
            }
        },
        showMessage:function(msg){
            this.$el.find("#backupMessage").text(msg);
        },
        handleButtonClickForTimestampRevert:function(selectElem, backupType){
            var node = this.backupModel.get("node");
            var value = this.$el.find(selectElem).val();
            var params = {
                type:backupType,
                node:node,
                time:value
            };
            this.handleButtonClickRaw(node, params);
        },
        handleButtonClickForRevisionRevert:function(rev){
            var node = this.backupModel.get("node");
            var params = {
                type:"gold",
                node:node,
                revision:rev
            };
            this.handleButtonClickRaw(node, params);
        },
        handleButtonClickRaw:function(node, params){
            var name = node > -1 ? "Task " + node : "Job";
            this.model.revert(params).done(function(data,result,xhr){
                if (params.hasOwnProperty("revision")) {
                    alertify.message("Attempted to revert to a previous run (which may not exist)");
                } else {
                    alertify.success(name + " reverted successfully.");
                }
            }).fail(function(xhr){
                alertify.error("Error reverting " + name + ":<br/>" + xhr.responseText);
            });
        },
        handleRunButtonClick:function(event){
            rev = this.$el.find("#runSelect").val();
            this.handleButtonClickForRevisionRevert(rev);
        },
        handleGoldButtonClick:function(event){
            this.handleButtonClickForTimestampRevert("#goldSelect", "gold");
        },
        handleHourlyButtonClick:function(event){
            this.handleButtonClickForTimestampRevert("#hourlySelect", "hourly");
        },
        handleDailyButtonClick:function(event){
            this.handleButtonClickForTimestampRevert("#dailySelect", "daily");
        },
        handleWeeklyButtonClick:function(event){
            this.handleButtonClickForTimestampRevert("#weeklySelect", "weekly");
        }
    });
    var DetailView = Backbone.View.extend({
        className:"detail-view",
        events:{
            "click #kickJobButton":"handleKickButtonClick",
            "click #rebalanceJobButton":"handleRebalanceButtonClick",
            "click #enableJobButton":"handleEnableButtonClick",
            "click #disableJobButton":"handleDisableButtonClick",
            "click #fixDirsJobButton":"handleFixDirsButtonClick",
            "click #queryJobButton":"handleQueryButtonClick",
            "click #deleteJobButton":"handleDeleteButtonClick",
            "click #stopJobButton":"handleStopButtonClick",
            "click #killJobButton":"handleKillButtonClick",
            "click #checkDirsJobButton":"handleCheckDirsJobButton",
            "click #commitJobButton":"handleCommitJobButton",
            "click #revertJobButton":"handleRevertJobButtonClick",
            "click #saveJobButton":"handleSaveJobButtonClick",
            "click #settingsChangePermission":"handleSettingsChangePermissionClick",
            "click #validateLink":"handleValidateClick",
            "click li.disabled > a":"handleDisabledTabClick",
            "click #cloneJobButton":"handleCloneClick"
        },
        initialize:function(options){
            options = options || {};
            _.bindAll(this,'render','template','handleSaveJobButtonClick','handleCloneClick');
            this.isClone=(_.has(options,"isClone")?options.isClone:false);
            this.configModel = options.configModel;
            this.parameterCollection = options.parameterCollection;
            this.listenTo(app.jobCollection,"change:qc_canQuery",this.handleCanQueryChange);
            this.listenTo(app.jobCollection,"change:stateText",this.handleStateChange);
            this.listenTo(app.jobCollection,"change:status",this.handleStatusChange);
        },
        detailTemplate: _.template(jobDetailTemplate),
        render:function(){
            var html = this.detailTemplate({
                job:this.model.toJSON(),
                isClone:this.isClone,
                cloneId:this.model.cloneId
            });
            this.$el.html(html);
            this.$el.find("ul.nav.nav-tabs li.active").removeClass("active");
            return this;
        },
        handleDisabledTabClick:function(event){
            event.preventDefault();
            event.stopImmediatePropagation();
        },
        handleCloneClick:function(event){
            event.preventDefault();
            if (this.model.attributes.dontCloneMe) {
                alertify.alert("Job with id "+this.model.id+" has \"do not clone\" parameter enabled.");
            } else {
                app.router.navigate("#jobs/"+this.model.id+"/conf/clone",{trigger:true});
            }
        },
        handleCommitJobButton:function(event){
            event.preventDefault();
            var self=this;
            alertify.prompt("Enter commit message:","",function(evt, str){
                self.model.commit=str;
                self.handleSaveJobButtonClick(event);
            });
        },
        handleStatusChange:function(model){
            var statusBox = this.$el.find("#statusBox");
            if(_.isEqual(model.id,this.model.id)){
                this.model.set("status",model.get("status"));
                statusBox.html(this.model.get("status"));
            }
        },
        handleValidateClick:function(event){
            event.preventDefault();
            event.stopImmediatePropagation();
            var config = this.configModel.get("config");
            var params = this.parameterCollection.toJSON();
            var self=this;
            this.model.validate(config,params).done(function(data){
                var log,model=self.model;
                if(data.result=="preExpansionError"){
                    log=alertify.error(data.message, 60);
                }
                else if(data.result=="postExpansionError"){
                    log=alertify.error(data.message, 60);
                    if (!_.isEmpty(model.cloneId)){
                        app.router.navigate("#jobs/"+model.cloneId+"/clone/expanded",{trigger:true});
                    }
                    else if(_.isUndefined(model.id)){
                        app.router.navigate("#jobs/create/expanded",{trigger:true});
                    }
                    else{
                        app.router.navigate("#jobs/"+model.id+"/expanded",{trigger:true});
                    }
                }
                else{
                    log=alertify.success("Job is valid.");
                }
                //Spawn.updateSingleCategoryAlerts('validation', log)
            }).fail(function(xhr){
                alertify.error("Error requesting job validation.");
            });
        },
        handleStateChange:function(model){
            var stateBox = this.$el.find("#stateTextBox");
            if(_.isEqual(model.id,this.model.id)){
                this.model.set("state",model.get("state"));
                this.model.set("stateText",model.get("stateText"));
                this.model.set("stateLabel",model.get("stateLabel"));
                stateBox.attr("class","label "+this.model.get("stateLabel"));
                stateBox.html(this.model.get("stateText"));
            }
        },
        handleCanQueryChange:function(model){
            if(_.isEqual(model.id,this.model.id)){
                this.model.set("qc_canQuery",model.get("qc_canQuery"));
                if(this.model.get("qc_canQuery")){
                    this.$el.find("#queryJobButton").show();
                }
                else{
                    this.$el.find("#queryJobButton").hide();
                }
            }
        },
        handleKickButtonClick:function(event){
            event.preventDefault();
            var self = this;
            if (this.configModel) {
                var tempConfig = this.configModel.get("config");
                var config = this.configModel.get("savedConfig");
                if (tempConfig !== config) {
                        alertify.warning("Warning: kicking job that may have unsaved changes!", 12);
                }
            }
            var params = this.parameterCollection.toJSON();
            var confirmIfInvalid = function(){
                var validatePromise = self.model.validate(config,params);
                validatePromise.done(function(data){
                    if (data.result=="preExpansionError" || data.result=="postExpansionError") {
                        alertify.confirm("Job failed validation, are you sure you want to kick?", function (e) {
                            self.model.kick();
                        });
                    } else {
                        self.model.kick();
                    }
                }).fail(function(data){
                    alertify.confirm("Something went wrong with checking validation, do you still want to kick?", function (e) {
                        self.model.kick();
                    });
                });
            };
            if(app.isQuiesced){
                alertify.confirm("Cluster is quiesced, are you sure you want to kick job '"+this.model.get("description")+"'?", function (e) {
                    confirmIfInvalid();
                });
            }else{
                confirmIfInvalid();
            }
        },
        handleRebalanceButtonClick:function(event){
            event.preventDefault();
            this.model.rebalance();
        },
        handleEnableButtonClick:function(event){
            event.preventDefault();
            // shift click triggers unsafe enable!
            this.model.enable(event.shiftKey);
        },
        handleDisableButtonClick:function(event){
            event.preventDefault();
            this.model.disable();
        },
        handleFixDirsButtonClick:function(event){
            event.preventDefault();
            this.model.fixDirs();
        },
        handleQueryButtonClick :function(event){
            event.preventDefault();
            this.model.query();
        },
        handleDeleteButtonClick :function(event){
            event.preventDefault();
            event.stopImmediatePropagation();
            var self=this;
            alertify.confirm("Are you sure you would like to delete job '"+this.model.get("description")+"'?", function (e) {
                self.model.delete();
            });
        },
        handleStopButtonClick:function(event){
            event.preventDefault();
            this.model.stop();
        },
        handleKillButtonClick:function(event){
            event.preventDefault();
            this.model.kill();
        },
        handleCheckDirsJobButton:function(event){
            event.preventDefault();
            app.router.trigger("showCheckDirs",this.model.id);
        },
        handleRevertJobButtonClick:function(event){
            event.preventDefault();
            app.router.trigger("route:showJobBackups",this.model.id);
        },
        handleSettingsChangePermissionClick:function(event){
            var parameters = {}
            parameters["jobs"] = this.model.id;
            parameters["owner"] = $('#jobOwner').val();
            parameters["group"] = $('#jobGroup').val();
            parameters["ownerWritable"] = $('#ownerWritable').is(':checked');
            parameters["groupWritable"] = $('#groupWritable').is(':checked');
            parameters["worldWritable"] = $('#worldWritable').is(':checked');
            parameters["ownerExecutable"] = $('#ownerExecutable').is(':checked');
            parameters["groupExecutable"] = $('#groupExecutable').is(':checked');
            parameters["worldExecutable"] = $('#worldExecutable').is(':checked');
            app.authQueryParameters(parameters);
            $.ajax({
                url: "/job/permissions",
                type: "POST",
                data: parameters,
                dataType: "json"
            }).done(function(data) {
               if (data.changed.length > 0) {
                    alertify.success("job permissions have been updated");
                }
                if (data.unchanged.length > 0) {
                    alertify.message("job permissions already had these changes");
                }
                if (data.notFound.length > 0) {
                    alertify.error("job not found");
                }
                if (data.notPermitted.length > 0) {
                    alertify.error("insufficient privileges");
                }
            }).fail(function(e){
                alertify.error("Error changing permissions" + e.responseText);
            });
        },
        handleSaveJobButtonClick:function(event){
            this.$el.find("#saveJobButton").addClass("disabled");
            var self = this;
            var formData = {};
            var config = this.configModel.get("config");
            var params = this.parameterCollection.toJSON();
            _.each(params, function(param){
                var name = "sp_"+param.name;
                formData[name]=param.value;
            });
            formData.config = config;
            var validatePromise = this.model.validate(config,params);
            validatePromise.done(function(data){
                if(data.result=="preExpansionError" || data.result=="postExpansionError"){
                    alertify.warning("Warning: saving job that failed validation!<br>" + data.message, 12);
                }
            }).fail(function(data){
                alertify.error("Error requesting job validation.");
            });
            this.model.save(formData).done(function(resp){
                alertify.success(resp.id + " saved successfully.",2)
                self.configModel.set("savedConfig", config);
                self.model.trigger("save.done");
                self.model.commit="";
                if(self.model.isNew()){
                    self.model.set("id",resp.id);
                    self.model.fetch({
                        success:function(model){
                            app.jobCollection.add(model);
                            app.job=undefined;
                            var location = window.location.hash;
                            if(self.isClone){
                                location=location.replace(self.model.cloneId,resp.id).replace("/clone","");
                            }else{
                                location=location.replace("create",resp.id);
                            }
                            self.$el.find("#saveJobButton").removeClass("disabled");
                            app.router.navigate(location,{trigger:true});
                        },
                        error:function(xhr){
                            alertify.error("Error loading job data for: "+resp.id);
                        }
                    });
                }
                else{
                    self.$el.find("#saveJobButton").removeClass("disabled");
                }

            }).fail(function(xhr){
                alertify.error("Error saving job: "+ xhr.responseText);
                self.model.trigger("save.error");
                self.$el.find("#saveJobButton").removeClass("disabled");
            });
        }
    });
    var ConfDetailView = DetailView.extend({
        template: _.template(jobConfigurationTemplate),
        events: _.defaults({
            'click a#hideParamLink':'handleParamChange',
            'change input':'handleInputChange',
            'change select#command':'handleCommandInputChange'
        }, DetailView.prototype.events),
        initialize:function(options){
            if (options.isClone) {
                options.model.attributes.dontDeleteMe = false;
                options.model.attributes.onComplete = "";
                options.model.attributes.onCompleteTimeout = 0;
                options.model.attributes.onError = "";
                options.model.attributes.onErrorTimeout = 0;
            }
            DetailView.prototype.initialize.apply(this,[options]);
            _.bindAll(this,'render','template');
            this.configModel=options.configModel;
            this.commandCollection=options.commandCollection;
            this.listenTo(this.parameterCollection,"reset",this.handleParamChange);
            this.listenTo(this.configModel,"reset",this.handleParamChange);
            this.listenTo(this.commandCollection,"reset",this.handleCommandsChange);
        },
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            var main = this.$el.find("div#detailContainer");
            main.html(this.template({
                job:this.model.toJSON(),
                commands:this.commandCollection.toJSON()
            }));
            main.find("#command").val(this.model.get("command"));
            this.views = {
                editor: new Editor.AceView({
                    model:this.configModel,
                    keyName:"config"
                }).render(),
                paramBox:new ParameterView({
                    model:this.model,
                    collection:this.parameterCollection
                }).render()
            };
            //adjust height
            main.append(this.views.editor.$el);
            main.append(this.views.paramBox.$el);
            this.$el.find("ul.nav.nav-tabs li#confTab").addClass("active");
            this.handleParamChange();
            this.resize();
            return this;
        },
        handleCommandsChange:function(){
            var optionsHtml="";
            _.each(this.commandCollection.toJSON(),function(command){
                optionsHtml+="<options value='"+command.name+"'>"+command.name+"</options>";
            });
            var command = this.$el.find("#command");
            command.html(optionsHtml);
            command.val(this.model.get("command"));
        },
        handleCommandInputChange:function(event){
            var select = $(event.currentTarget);
            var value = select.val();
            this.model.set("command",value);
        },
        handleParamChange:function(){
            this.resize();
            this.views.paramBox.render();
            this.views.editor.$el.css({
                bottom:this.views.paramBox.$el.height()
            });
            this.views.editor.views.editor.resize();
        },
        handleInputChange:function(event){
            var input = $(event.currentTarget);
            var value = input.val();
            var name = input.attr("name");
            this.model.set(name,value);
        },
        resize:function(event){
            this.views.editor.$el.css({
                position:"absolute",
                top:'59px',
                bottom:this.views.paramBox.$el.height(),
                right:0,
                left:0
            });
            this.views.paramBox.$el.css({
                position:"absolute",
                bottom:0,
                right:0,
                left:0,
                "z-index":100,
                "background-color":"white"
            });
        }
    });
    var SettingDetailView = DetailView.extend({
        template: _.template(jobSettingTemplate),
        events: _.defaults({
            "keyup input":"handleInputKeyUp",
            "change input":"handleInputKeyUp",
            "click input[type='checkbox']":"handleCheckboxClick"
        }, DetailView.prototype.events),
        initialize:function(options){
            DetailView.prototype.initialize.apply(this,[options]);
        },
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            var html = this.template({
                job:this.model.toJSON()
            });
            this.$el.find("div#detailContainer").html(html);
            this.$el.find("ul.nav.nav-tabs li#settingsTab").addClass("active");
            return this;
        },
        handleInputKeyUp:function(event){
            var input = $(event.currentTarget);
            var name = input.attr("name");
            var value = input.val();
            this.model.set(name,value);
        },
        handleCheckboxClick:function(event){
            var input = $(event.currentTarget);
            this.model.set(input.attr("name"),input.is(":checked"));
        }
    });
    var TaskTableView = DetailView.extend({
        template: _.template(jobTaskTableTemplate),
        initialize:function(options){
            DetailView.prototype.initialize.apply(this,[options]);
            this.taskCollection=options.taskCollection;
        },
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            this.views={
                table: new Task.TableView({
                    id:'jobTaskTable',
                    collection:this.taskCollection,
                    hostUrlTemplate: "#jobs/<%=jobUuid%>/tasks/<%=node%>",
                    allJobUrlTemplate:"#jobs/<%=jobUuid%>/tasks"
                })
            };
            var detail = this.$el.find("div#detailContainer");
            detail.append(this.views.table.$el);
            this.views.table.render();
            this.$el.find("ul.nav.nav-tabs li#tasksTab").addClass("active");
            return this;
        }
    });
    var AlertDetailView = DetailView.extend({
        template: _.template(jobAlertsTemplate),
        events: _.extend(DetailView.prototype.events,{
            "click #addAlertButton":"handleAddAlertButtonClick",
            "click #viewAlertsButton":"handleViewAlertsButtonClick",
            "keyup input[name='email']":"handleEmailKeyUp",
            "change select[name='type']":"handleSelectChange",
            "keyup input[name='timeout']":"handleTimeoutKeyUp",
            "click button.close":"handleCloseButtonClick"
        }),
        initialize:function(options){
            //this.listenTo(this.model,"change:alerts",this.render);
            this.listenTo(this.model,"alerts.add",this.render);
            this.listenTo(this.model,"save.start",this.render);
            DetailView.prototype.initialize.apply(this,[options]);
        },
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            var html = this.template({
                job:this.model.toJSON(),
                util:util
            });
            this.$el.find("div#detailContainer").html(html);
            this.$el.find("ul.nav.nav-tabs li#alertsTab").addClass("active");
            return this;
        },
        handleAddAlertButtonClick:function(event){
            app.router.navigate("#alerts/create/" + this.model.id, {trigger: true});
        },
        handleViewAlertsButtonClick:function(event){
        	app.router.navigate("#alertsFiltered/" + this.model.id, {trigger: true});
        },
        handleEmailKeyUp:function(event){
            var input = $(event.currentTarget);
            var index = input.data("index");
            var alerts = this.model.get("alerts");
            alerts[index].email=input.val();
        },
        handleSelectChange:function(event){
            var select = $(event.currentTarget);
            var index = select.data("index");
            var alerts = this.model.get("alerts");
            var value = parseInt(select.val());
            alerts[index].type=value;
            if(value<=1){
                alerts[index].timeout="";
            }
            this.render();
        },
        handleTimeoutKeyUp:function(event){
            var input = $(event.currentTarget);
            var index = input.data("index");
            var alerts = this.model.get("alerts");
            var value = parseInt(input.val());
            if(value>0){
                alerts[index].timeout=value;
            }
        },
        handleCloseButtonClick:function(event){
            var button = $(event.currentTarget);
            var index = parseInt(button.data("index"));
            var alerts = this.model.get("alerts");
            alerts.splice(index,1);
            this.model.set("alerts",alerts);
            this.render();
        }
    });
    var DependenciesDetailView = DetailView.extend({
        template: _.template(jobDependenciesTemplate),
        initialize:function(options){
            DetailView.prototype.initialize.apply(this,[options]);
            _.bindAll(this,'render','close');
            this.graphModel=options.graphModel;
            this.listenTo(this.model,"change",this.render);
            this.listenTo(this.graphModel,"change",this.handleGraphModelChange);
        },
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            this.views={
                graph: new Graph.TreeDependencyGraphView({
                    model:this.graphModel
                })
            };
            var detail = this.$el.find("div#detailContainer");
            this.views.graph.$el.css({
                "margin-left":"0px",
                "width":detail.width(),
                "height":detail.height()
            });
            detail.append(this.views.graph.$el);
            this.$el.find("ul.nav.nav-tabs li#depTab").addClass("active");
            return this;
        },
        handleGraphModelChange:function(){
            this.views.graph.render();
        },
        close:function(){
            this.$el.remove();
            return this;
        }
    });
    var ExpandedConfDetailView = DetailView.extend({
        initialize:function(options){
            DetailView.prototype.initialize.apply(this,[options]);
            _.bindAll(this,'render','handleConfigChange','handleParamChange');
            this.configModel=options.configModel;
            this.expandModel=options.expandModel;
            this.parameterCollection=options.parameterCollection;
            this.listenTo(this.configModel,"change:config",this.handleConfigChange);
            this.listenTo(this.expandModel,"change:expanded",this.render);
            this.listenTo(this.parameterCollection,"change",this.handleParamChange);
            this.listenTo(this.parameterCollection,"reset",this.handleParamChange);
        },
        template: _.template(jobExpandedConfTemplate),
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            var detail = this.$el.find("div#detailContainer");
            this.views = {
                editor: new Editor.AceView({
                    model:this.expandModel,
                    keyName:"expanded",
                    readOnly:true
                }).render()
            };
            //adjust height
            this.views.editor.$el.css({
                position:"absolute",
                top:0,
                bottom:0,
                right:0,
                left:0
            });
            detail.append(this.views.editor.$el);
            this.$el.find("ul.nav.nav-tabs li#expTab").addClass("active");
            return this;
        },
        handleConfigChange:function(){
            this.expandModel.set("config",this.configModel.get("config"));
            this.expandModel.fetch();
        },
        handleParamChange:function(){
            var self=this;
            _.each(self.parameterCollection.toJSON(),function(param){
                self.expandModel.set("sp_"+param.name,param.value);
            });
            self.expandModel.fetch();
        }
    });
    var CheckDirsModel = Backbone.Model.extend({
        defaults:{
            "hostId": "",
            "isReplica": "",
            "jobKey": {
                "jobUuid": "",
                "nodeNumber": ""
            },
            "type": ""
        }
    });
    var CheckDirsCollection = Backbone.Collection.extend({
        initialize:function(options){
            //this.jobUuid=options.jobUuid;
        },
        sync: function(method, model, options){
            if(_.isEqual(method,'read')){
                var self=this;
                var ajax = $.ajax({
                    url: "/job/checkJobDirs",
                    data:{id:self.jobUuid}
                }).done(function(data){
                    self.set(data);
                    self.trigger("change");
                });
                return ajax;
            }
        },
        model:CheckDirsModel
    });
    var CheckDirsModal = Backbone.View.extend({
        className:"modal fade",
        events:{
            "click button.fix":"handleFixButtonClick",
            "click button.fix-all":"handleFixAllDirsButtonClick"
        },
        initialize:function(options){
            _.bindAll(this,'handleCollectionChange');
            this.listenTo(this.collection,"change",this.handleCollectionChange);
        },
        template: _.template(jobCheckDirsTemplate),
        render:function(){
            var html = this.template({
                dirs:this.collection.toJSON(),
                job:this.model.toJSON()
            });
            this.$el.html(html);
            return this;
        },
        handleFixAllDirsButtonClick:function(event){
            this.model.fixDirs(-1);
        },
        handleFixButtonClick:function(event){
            var button = $(event.currentTarget);
            var jobId = button.data("job"), node=parseInt(button.data("node"));
            this.model.fixDirs(node);
        },
        handleCollectionChange:function(){
            var html = "";
            _.each(this.collection.toJSON(),function(match,index){
                html+="<tr class='row'>"+
                        "<td>"+
                            match.jobKey.nodeNumber+
                        "</td>"+
                        "<td>"+
                            (match.isReplica?"Replica":"Live")+
                        "</td>"+
                        "<td>"+
                            util.generateTaskDirStatusText(match.type)+
                        "</td>"+
                        "<td>"+
                            match.hostId+
                        "</td>"+
                        "<td>"+
                            ((match.type != "MATCH" && match.type != "REPLICATION_IN_PROGRESS") ? "<button class='btn btn-default btn-small fix' data-job='"+match.jobKey.jobUuid+"' data-node='"+match.jobKey.nodeNumber+"' class='btn btn-default btn-small'>Fix</button>":"<button class='btn btn-default btn-small disabled'>Fix</button>")+
                        "</td>"+
                    "</tr>";
            });
            this.$el.find("table tbody").html(html);
            this.$el.modal("show");
        }
    });
    var HistTableView = DataTable.View.extend({
        initialize:function(options){
            var self=this;
            options = options || {};
            this.id = options.id || "jobHistTable";
            var columns =[
                {
                    "sTitle":"Commit",
                    "sClass":"hist-commit",
                    "mData": "commit",
                    "sWidth":"25%",
                    "mRender":function(val,type,data){
                        return "<a href='#jobs/"+self.collection.jobUuid+"/history/"+val+"'>"+val+"</a>";
                    }
                },
                {
                    "sTitle":"Time",
                    "sClass":"hist-time",
                    "mData": "time",
                    "sWidth":"25%",
                    "mRender":function(val,type,data){
                        return util.convertToDateTimeText(val);
                    }
                },
                {
                    "sTitle":"Message",
                    "sClass":"hist-msg",
                    "mData": "msg",
                    "sWidth":"40%"
                },
                {
                    "sTitle":"",
                    "sClass":"hist-actions",
                    "sWidth":"10%",
                    "mData":"commit",
                    "bSearchable":false,
                    "bSortable":false,
                    "mRender":function(val,type,data){
                        var html="<a href='#jobs/"+self.collection.jobUuid+"/history/"+val+"/diff' class='btn btn-default btn-tiny conf-diff' data-commit='"+val+"'>Diff</a>";
                        //html+="<button class='btn btn-default btn-tiny conf-view' data-commit='"+val+"'>View</button>";
                        html+="<a class='btn btn-default btn-tiny conf-load' data-commit='"+val+"'>Load</a>";
                        return html;
                    }
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                heightBuffer:80,
                columnFilterIndex:1,
                id:this.id,
                emptyMessage:" "
            }]);
        }
    });
    var HistoryDetailView = DetailView.extend({
        events: _.extend(DetailView.prototype.events,{
            "click a.conf-load":"handleCommitLoad"
        }),
        template: _.template(jobHistoryTemplate),
        initialize:function(options){
            _.bindAll(this,'handleCommitLoad');
            DetailView.prototype.initialize.apply(this,[options]);
            this.historyCollection=options.historyCollection;
        },
        render:function(dontAppend){
            DetailView.prototype.render.apply(this,[]);
            this.views={
                table: new HistTableView({
                    id:'jobHistTable',
                    collection:this.historyCollection
                })
            };
            if(!dontAppend){
                var detail = this.$el.find("div#detailContainer");
                detail.append(this.views.table.$el);
                this.views.table.render();
            }
            this.$el.find("ul.nav.nav-tabs li#historyTab").addClass("active");
            return this;
        },
        handleCommitLoad:function(event){
            event.preventDefault();
            var self=this;
            var button = $(event.currentTarget);
            var jobId = this.model.id;
            var commit = button.data("commit");
            var commitModel = this.historyCollection.get(commit);
            commitModel.load().done(function(data){
                self.configModel.set("config",data);
                alertify.success("Loaded config from "+commit+". Save the job to finalize the change.");
                app.router.navigate("#jobs/"+jobId+"/conf",{trigger:true});
            }).fail(function(xhr){
                alertify.error("Error loading config for commit: "+commit);
            });
            //this.configModel.set("config",c)
        }
    });
    var HistoryCommitView = HistoryDetailView.extend({
        initialize:function(options){
            HistoryDetailView.prototype.initialize.apply(this,[options]);
            this.commitModel=options.commitModel;
            this.editorAttribute = (_.isUndefined(options.editorAttribute)?"historyConfig":"diff");
        },
        render:function(){
            var self=this;
            HistoryDetailView.prototype.render.apply(this,[true]);
            this.views.editor = new Editor.AceView({
                model:this.commitModel,
                keyName:self.editorAttribute,
                readOnly:true
            });
            this.views.layout = new Layout.HorizontalSplit({
                topView:self.views.table,
                bottomView:self.views.editor,
                topHeight:30,
                bottomHeight:70
            }).render();
            var detail = this.$el.find("div#detailContainer");
            detail.append(this.views.layout.$el);
            this.views.editor.$el.height("100%");
            return this;
        }
    });
    var ParameterView = Backbone.View.extend({
        template: _.template(jobParameterTemplate),
        events:{
            "click a#hideParamLink":"handleHideParamClick",
            "change input":"handleInputKeyUp",
            "keyup input":"handleInputKeyUp"
        },
        initialize:function(){
        },
        render:function(){
            var cookie = Cookies.get("hideParam");
            var html = this.template({
                hidden:_.isEqual(cookie,1),
                parameters:this.collection.toJSON()
            });
            this.$el.html(html);
            return this;
        },
        handleHideParamClick:function(event){
            var val = this.$el.find("a#hideParamLink").data("hide");
            var hideVal = (parseInt(val)+1)%2;
            Cookies.set("hideParam", hideVal);
        },
        handleInputKeyUp:function(event){
            var input = $(event.currentTarget);
            //console.log(input.attr("name")+" just changed: "+input.val());
            var param = this.collection.get(input.attr("name"));
            if(!_.isUndefined(param)){
                param.set("value",input.val());
            }
        }
    });
    var TaskDetailView = DetailView.extend({
        template: _.template(jobTaskDetailTemplate),
        initialize:function(options){
            DetailView.prototype.initialize.apply(this,[options]);
            this.taskModel = options.taskModel;
            this.logModel = options.logModel;
            this.taskCollection = options.taskCollection;
        },
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            var detail = new Task.DetailView({
                model:this.taskModel,
                log:this.logModel
            });
            var table = new Task.TinyTableView({
                id:"taskTable"+this.model.id,
                collection:this.taskCollection,
                model:this.taskModel,
                nodeNumber:this.taskModel.get("node"),
                enableSearch:false,
                hostUrlTemplate: "#jobs/<%=jobUuid%>/tasks/<%=node%>",
                breadcrumbTemplate: _.template(jobTaskBreadcrumbTemplate)
            });
            this.views={
                table: table,
                detail: detail,
                layout: new Layout.VerticalSplit({
                    rightView:detail,
                    leftView:table,
                    rightWidth:80,
                    leftWidth:20
                })
            };
            var container = this.$el.find("div#detailContainer");
            container.append(this.views.layout.$el);
            this.views.layout.render();
            this.$el.find("ul.nav.nav-tabs li#tasksTab").addClass("active");
            return this;
        },
        remove:function(){
            this.views.layout.remove();
            this.$el.html("");
        }
    });
    return {
        AlertDetailView:AlertDetailView,
        BackupModalView:BackupModalView,
        ChangePermissionsModalView:ChangePermissionsModalView,
        BackupModel: BackupModel,
        CheckDirsCollection:CheckDirsCollection,
        CheckDirsModal:CheckDirsModal,
        ConfigModel:ConfigModel,
        Collection:Collection,
        ComfyTableView:ComfyJobTable,
        CompactTable:CompactTable,
        ConfDetailView:ConfDetailView,
        DependenciesDetailView:DependenciesDetailView,
        DetailView: DetailView,
        ExpandedConfDetailView:ExpandedConfDetailView,
        ExpandedConfigModel:ExpandedConfigModel,
        HistoryModel:HistoryModel,
        HistoryCollection:HistoryCollection,
        HistoryDetailView:HistoryDetailView,
        HistoryCommitView:HistoryCommitView,
        InfoMetricModel: InfoMetricModel,
        InfoMetricView: InfoMetricView,
        JobTable:JobTable,
        Model: Model,
        ParameterCollection:ParameterCollection,
        ParameterModel:ParameterModel,
        ParameterView:ParameterView,
        SettingDetailView:SettingDetailView,
        TaskDividerView:TaskDividerView,
        TaskDetailDividerView:TaskDetailDividerView,
        TaskTableView:TaskTableView,
        TaskDetailView:TaskDetailView
    };
});
