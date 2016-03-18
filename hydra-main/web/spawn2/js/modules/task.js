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
    "text!../../templates/task.filter.html",
    "text!../../templates/task.selectable.html",
    "text!../../templates/task.breadcrumbs.html",
    "text!../../templates/task.detail.html"
],
function(
    app,
    alertify,
    Cookies,
    DataTable,
    util,
    taskFilterTemplate,
    taskSelectableTemplate,
    taskBreadcrumbsTemplate,
    taskDetailTemplate
){
    var States=[
        "IDLE",
        "BUSY",
        "ERROR",
        "ALLOCATED",
        "BACKUP",
        "REPLICATE",
        "UNKNOWN",
        "REBALANCE",
        "REVERT",
        "QUEUED, WAITING ON UNAVAIL HOST",
        "SWAPPING",
        "QUEUED",
        "MIGRATING",
        "FULL REPLICATE",
        "QUEUED, WAITING ON TASK SLOT"
    ];
    var StateLabels=[
        "label-default",//idle
        "label-success",//busy
        "label-danger",//error
        "label-info",//allocated
        "label-success",//backup
        "label-success",//replicate
        "label-inverse",//unknown
        "label-info",//rebalance
        "label-success",//revert
        "label-info",//disk full
        "label-info",//swapping
        "label-info",//queued
        "label-success",//migrating
        "label-success",//full replicate
        "label-info",//queued-no-slot
    ];
    var errorCodes={

    };
    var Model = Backbone.Model.extend({
        idAttribute:"node",
        initialize:function(options){
            _.bindAll(this,'handleHostRemove');
            this.listenTo(app.hostCollection,"remove",this.handleHostRemove);
            this.listenTo(app.hostCollection,"add",this.handleHostAdd);
        },
        url:function(){
            return "/task/get?job="+this.get("jobUuid")+"&task="+this.id;
        },
        parse:function(data){
            data.DT_RowId=""+data.node;
            data.DT_RowClass='task-row';
            data.node=data.node;
            data.state = (_.isNumber(data.state)?data.state:6);
            data.stateText = States[data.state];
            data.stateLabel = StateLabels[data.state];
            data.replicas = (_.isArray(data.replicas)?data.replicas.length:0);
            data.hostUuid = data.hostUuid || "";
            if(!_.isUndefined(app.hostCollection.get(data.hostUuid))){
                var host = app.hostCollection.get(data.hostUuid);
                data.host = host.get("host");
                data.hostPort = host.get("port");
            }
            else{
                data.host="n/a";
            }
            return data;
        },
        defaults:{
            node:"",
            stateText:States[6],
            state:6,
            host:"",
            hostUuid:"",
            errorCode:"",
            replicas:"",
            input:"",
            skipped:"",
            totalEmitted:"",
            meanRate:"",
            fileCount:"",
            fileBytes:""

        },
        handleHostRemove:function(host){
            if(_.isEqual(host.id,this.get("hostUuid"))){
                this.set("host","n/a");
            }
        },
        handleHostAdd:function(host){
            if(_.isEqual(host.id,this.get("hostUuid"))){
                var host = app.hostCollection.get(host.id);
                this.set("host",host.get("host"));
            }
        },
        stop:function(notify){
            notify = notify || false;
            var self = this;
            $.ajax({
                url: "/task/stop",
                data:{
                    job:this.get("jobUuid"),
                    task:this.get("node")
                },
                type: "GET",
                dataType: "text"
            }).done(function(data){
                if(notify){
                    alertify.message("Task "+self.id+" stopped.");
                }
            }).fail(function(e){
                alertify.error("Error stopped task: "+self.id+" <br/>"+e.responseText);
            });
        },
        kick:function(notify, priority){
            priority = priority || 2;
            notify = notify || false;
            var self = this;
            $.ajax({
                url: "/task/start",
                data:{
                    job: this.get("jobUuid"),
                    task: this.get("node"),
                    priority: priority
                },
                type: "GET",
                dataType: "text"
            }).done(function(data){
                if(notify){
                    alertify.message("Task "+self.id+" kicked.");
                }
            }).fail(function(e){
                alertify.error("Error kicking task: "+self.id+" <br/>"+e.responseText);
            });
        },
        kill:function(notify){
            notify = notify || false;
            var self = this;
            $.ajax({
                url: "/task/kill",
                data:{
                    job:this.get("jobUuid"),
                    task:this.get("node")
                },
                type: "GET",
                dataType: "text"
            }).done(function(data){
                if(notify){
                    alertify.message("Task "+self.id+" killed.");
                }
            }).fail(function(e){
                alertify.error("Error killing task: "+self.id+" <br/>"+e.responseText);
            });
        }
    });
    var Collection = Backbone.Collection.extend({
        initialize:function(options){
            this.listenTo(app.server,'job.update',this.handleJobUpdate);
        },
        url:function(){
            return "/job/get?id="+this.jobUuid+"&field=nodes";
        },
        parse:function(collection){
            var array = new Array(collection.length);
            _.each(collection,function(model,idx){
                array[idx]= Model.prototype.parse(model);
            });
            return array;
        },
        model:Model,
        handleJobUpdate:function(job){
            if(!_.isUndefined(this.jobUuid) && _.isEqual(job.id,this.jobUuid)){
                this.fetch();
            }
        }
    });
    var TableView = DataTable.View.extend({
        initialize:function(options){
            _.bindAll(this,
                'handleKickButtonClick',
                'handleStopButtonClick',
                'handleKillButtonClick');
            options = options || {};
            this.id = options.id || "taskTable";
            this.hostUrlTemplate = (_.isEmpty(options.hostUrlTemplate)? _.template("#jobs/<%=jobUuid%>/quick/<%=node%>"): _.template(options.hostUrlTemplate));
            this.allJobUrlTemplate = (_.isEmpty(options.allJobUrlTemplate)? _.template("#jobs/<%=jobUuid%>/quick"):_.template(options.allJobUrlTemplate));
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "sTitle":"",
                    "sClass":"task-cb",
                    "sWidth":"2%",
                    "mData": "node",
                    "bSearchable":false,
                    "bSortable":false,
                    "mRender":function(val,type,data){
                        if(self.selectedIds[val]){
                            return "<input checked class='row_selectable' type='checkbox'></input>";
                        }
                        else{
                            return "<input class='row_selectable' type='checkbox'></input>";
                        }
                    }
                },
                {
                    "sTitle":"No",
                    "sClass":"task-node",
                    "mData": "node",
                    "sWidth":"4%"
                },
                {
                    "mData": "stateText",
                    "bVisible":false,
                    "bSearchable":true
                },
                {
                    "sTitle":"State",
                    "sClass":"task-state center",
                    "mData": "state",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return "<span class='label "+StateLabels[val]+"'>"+States[val]+"</span>";
                    },
                    "aDataSort":[2],
                    "aTargets":[3]
                },
                {
                    "sTitle":"Host",
                    "sClass":"task-host center",
                    "mData": "host",
                    "sWidth":"5%",//24
                    "mRender":function(val,type,data){
                        return "<a href='"+self.hostUrlTemplate(data)+"'>"+val+"</a>";
                    }
                },
                {
                    "sTitle":"Host UUID",
                    "sClass":"task-hostuuid center",
                    "mData": "hostUuid",
                    "sWidth":"5%"//45
                },
                {
                    "sTitle":"Status",
                    "sClass":"task-status center",
                    "mRender":function(val){
                        return util.statusTextForExitCode(val);
                    },
                    "mData": "errorCode",
                    "sWidth":"4%"//51
                },
                {
                    "sTitle":"Replicas",
                    "sClass":"task-rep",
                    "mData": "replicas",
                    "sWidth":"4%",//56
                    "bSearchable":false
                },
                {
                    "mData": "input",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"Input",
                    "sClass":"task-input center",
                    "mData": "input",
                    "sWidth":"5%",//63
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.shortenNumber(val);
                    },
                    "aDataSort":[8],
                    "aTargets":[9]
                },
                {
                    "mData": "skipped",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"Skipped",
                    "sClass":"task-rekt center",
                    "mData": "skipped",
                    "sWidth":"5%",//70
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.shortenNumber(val);
                    },
                    "aDataSort":[10],
                    "aTargets":[11]
                },
                {
                    "mData": "totalEmitted",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"Emitted",
                    "sClass":"task-nodes center",
                    "mData": "totalEmitted",
                    "sWidth":"5%",//77
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.shortenNumber(val);
                    },
                    "aDataSort":[12],
                    "aTargets":[13]
                },
                {
                    "mData": "meanRate",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"Rate",
                    "sClass":"task-rate center",
                    "mData": "meanRate",
                    "sWidth":"5%",//84
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.shortenNumber(val);
                    },
                    "aDataSort":[14],
                    "aTargets":[15]
                },
                {
                    "mData": "fileCount",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"Files",
                    "sClass":"task-errored center",
                    "mData": "fileCount",
                    "sWidth":"5%",//91
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.shortenNumber(val);
                    },
                    "aDataSort":[16],
                    "aTargets":[17]
                },
                {
                    "mData": "fileBytes",
                    "bVisible":false,
                    "bSearchable":false
                },
                {
                    "sTitle":"Size",
                    "sClass":"task-done center",
                    "mData": "fileBytes",
                    "sWidth":"5%",//98
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDFH(val);
                    },
                    "aDataSort":[18],
                    "aTargets":[19]
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:taskFilterTemplate,
                enableSearch:false,
                selectableTemplate:taskSelectableTemplate,
                heightBuffer:80,
                columnFilterIndex:1,
                id:this.id,
                emptyMessage:" ",
                idAttribute:"node",
                changeAttrs:[
                    "state",
                    "input",
                    "replicas",
                    "skipped",
                    "totalEmitted",
                    "meanRate",
                    "fileCount",
                    "fileBytes",
                    "hostUuid",
                    "host",
                    "errorCode",
                    "stateText"
                ]
            }]);
        },
        render:function(){
            DataTable.View.prototype.render.apply(this,[]);
            this.views.selectable.find("#kickTaskButton").on("click",this.handleKickButtonClick);
            this.views.selectable.find("#stopTaskButton").on("click",this.handleStopButtonClick);
            this.views.selectable.find("#killTaskButton").on("click",this.handleKillButtonClick);
            return this;
        },
        handleKickButtonClick:function(event){
            var ids = this.getSelectedIds(),self=this;
            if (app.isQuiesced) {
                alertify.confirm("Cluster is quiesced, do you want to kick " + ids.length + " task(s) with extra priority?", function (e) {
                    _.each(ids,function(id){
                        self.collection.get(id).kick(false, 100);
                    });
                    alertify.message("Kicked "+ids.length+" tasks.");
                });
            } else {
                _.each(ids,function(id){
                    self.collection.get(id).kick(false, 2);
                });
                alertify.message("Kicked "+ids.length+" tasks.");
            }
        },
        handleStopButtonClick:function(event){
            var ids = this.getSelectedIds(),self=this;
            _.each(ids,function(id){
                self.collection.get(id).stop(false);
            });
            alertify.message("Stopped "+ids.length+" tasks.");
        },
        handleKillButtonClick:function(event){
            var ids = this.getSelectedIds(),self=this;
            _.each(ids,function(id){
                self.collection.get(id).kill(false);
            });
            alertify.message("Killed "+ids.length+" tasks.");
        }
    });
    var TinyTableView = DataTable.View.extend({
        initialize:function(options){
            options = options || {};
            this.id = options.id || "taskTable";
            this.nodeNumber = options.nodeNumber;
            this.breadcrumbTemplate = options.breadcrumbTemplate || _.template(taskBreadcrumbsTemplate);
            this.hostUrlTemplate = (_.isEmpty(options.hostUrlTemplate)? _.template("#jobs/<%=jobUuid%>/quick/<%=node%>"): _.template(options.hostUrlTemplate));
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "sTitle":"No",
                    "sClass":"task-node",
                    "mData": "node",
                    "bVisible":true,
                    "sWidth":"20%"
                },
                {
                    "mData": "stateText",
                    "bVisible":false,
                    "bSearchable":true
                },
                {
                    "sTitle":"State",
                    "sClass":"task-state center",
                    "mData": "state",
                    "sWidth":"40%",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return "<span class='label "+StateLabels[val]+"'>"+States[val]+"</span>";
                    },
                    "aDataSort":[1],
                    "aTargets":[2]
                },
                {
                    "sTitle":"Host",
                    "sClass":"task-host center",
                    "mData": "host",
                    "sWidth":"40%",//24
                    "bVisible":true,
                    "mRender":function(val,type,data){
                        //return "<a href='#jobs/"+data.jobUuid+"/quick/"+data.node+"'>"+val+"</a>";
                        return "<a href='"+self.hostUrlTemplate(data)+"'>"+val+"</a>";
                    }
                }
            ];
            var breadcrumbTemplate = this.breadcrumbTemplate({
                task:this.model.toJSON()
            });
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                selectableTemplate:breadcrumbTemplate,
                heightBuffer:80,
                columnFilterIndex:1,
                enableSearch:false,
                id:this.id,
                idAttribute:"node",
                emptyMessage:" "
            }]);
            this.listenTo(this.model,"change",this.handleTaskModelChange);
        },
        handleTaskModelChange:function(model){
            console.log("task change");
            var breadcrumbTemplate = this.breadcrumbTemplate({
                task:this.model.toJSON()
            });
            this.views.parent.find("div.selectable_action").html(breadcrumbTemplate);
        }
    });
    var DetailView = Backbone.View.extend({
        template: _.template(taskDetailTemplate),
        events:{
            "keyup input#linesInput":"handleLineInputChange",
            "change input#linesInput":"handleLineInputChange",
            "keyup input#runsAgoInput":"handleRunsAgoInputChange",
            "change input#runsAgoInput":"handleRunsAgoInputChange",
            "click div.log-control-button button":"handleLogControlClick",
            "click div.log-types button:not(.active)":"handleLogTypeClick",
            "click #kickTaskButton":"handleKickButtonClick",
            "click #stopTaskButton":"handleStopButtonClick",
            "click #killTaskButton":"handleKillButtonClick",
            "click #revertTaskButton":"handleRevertButtonClick"
        },
        initialize:function(options){
            _.bindAll(
                this,
                'render',
                'handleLineInputChange',
                'saveState',
                'handleLogChange',
                'handleTaskReset',
                'handleLogError',
                'handleKickButtonClick',
                'handleStopButtonClick',
                'handleKillButtonClick');
            var state = Cookies.getJSON("spawn") || {};
            state.log = state.log || {
                lines: 10,
                type: 0,
                stdout:true,
                runsAgo: 0
            };
            this.lines = state.log.lines;
            this.type = state.log.type; //0: roll, 1:tail, 2:head
            this.stdout = state.log.stdout;
            this.runsAgo = state.log.runsAgo;
            this.log = options.log;
            this.xhr=null;
            this.rollTimeout=null;
            this.saveState();
            this.listenTo(app.jobCollection,"change:state",this.handleJobStateUpdate);
            this.listenTo(this.model,"change:state",this.handleTaskStateChange);
            this.listenTo(this.model,"change:host",this.handleHostNameChange);
            this.listenTo(this.model,"change:hostPort",this.handleHostPortChange);
            this.listenTo(this.model,"reset",this.handleTaskReset);
            this.listenTo(this.log,"change",this.handleLogChange);
            this.listenTo(this.log,"clear",this.handleLogClear);
        },
        render:function(){
            var html = this.template({
                task:this.model.toJSON(),
                lines:this.lines,
                type:this.type,
                stdout:this.stdout,
                runsAgo:this.runsAgo
            });
            this.$el.html(html);
            //fetch log content after rendering
            this.log.lines=this.lines;
            this.log.type=this.type;
            this.log.stdout=this.stdout;
            this.log.runsAgo=this.runsAgo;
            this.$el.css("display", "flex");
            this.$el.css("height", "100%");
            this.$el.css("flex-direction", "column");
            return this;
        },
        handleRevertButtonClick:function(event){
            app.router.trigger("route:showJobBackups",this.model.get("jobUuid"),this.model.get("node"));
        },
        handleJobStateUpdate:function(model){
            if(_.isEqual(model.id,this.model.get("jobUuid"))){
                this.model.fetch();
            }
        },
        handleKickButtonClick:function(){
            var self = this;
            if (app.isQuiesced) {
                alertify.confirm("Cluster is quiesced, do you want to kick " + self.model.get("jobUuid") +
                        "/" + self.model.get("node") + " with extra priority?", function (e) {
                    self.model.kick(true, 100);
                });
            } else {
                this.model.kick(true, 2);
            }
        },
        handleStopButtonClick:function(){
            this.model.stop(true);
        },
        handleKillButtonClick:function(){
            this.model.kill(true);
        },
        replaceLink:function (match, p1, p2, offset, string) {
            p1 = p1.replace(/</g, "&lt;").replace(/>/g, "&gt;");
            var p1b = p1.split('.').slice(0,-2).join("/");
            if (p1b.length > 0){
                p1b = p1b + '/';
            }
            return p1 + '(<a class="ide-link" href="/?message=' + p1b + p2 + '">' + p2 + '</a>)'
        },
        linkFiles:function (text) {
            text = text.replace(/([\w.<>]+)\((\w+\.java:\d+)\)/g, this.replaceLink);
            return text;
        },
        handleLogClear:function(){
            if(!_.isNull(this.xhr)){
                this.xhr.abort();
                this.xhr=null;
            }
            if(!_.isNull(this.rollTimeout)){
                clearTimeout(this.rollTimeout);
                this.rollTimeout=null;
            }
            this.$el.find("pre#logContainer").html("loading...");
        },
        handleLogChange:function(log){
            var text = log.get("out");
            text = this.linkFiles(text);
            if(_.isEqual(this.type,0)){//roll
                var oldOffset = log.offset;
                var newOffset = log.get("offset");
                if (!oldOffset || newOffset < oldOffset) {
                    this.$el.find("pre#logContainer").html("");
                }
                var isAtBottom = this.isLogAtBottom();//before changing text
                this.$el.find("pre#logContainer").append(text);
                if(isAtBottom){
                    this.scrollLogToBottom();
                }
            }
            else if(_.isEqual(this.type,2)){//head
                this.$el.find("pre#logContainer").html("");
                this.$el.find("pre#logContainer").html(text);
                this.scrollLogToTop();
            }
            else{
                this.$el.find("pre#logContainer").html("");
                this.$el.find("pre#logContainer").html(text);
                this.scrollLogToBottom();
            }
            //bind links click event
            this.$el.find("pre#logContainer").find('a.ide-link').unbind().click(function(e) {
                e.preventDefault();
                var url = $(this).attr("href");
                $.getJSON('http://localhost:8091' + url + '&callback=?', function(json) {
                    //do nothing
                });
            });
        },
        roll:function(){
            this.log.offset=this.log.get("offset");
            this.xhr=this.log.fetch({error:this.handleLogError});
            var self=this;
            this.rollTimeout=setTimeout(function(){
                self.roll();
            },2000);
        },
        tail:function(){
            this.log.offset=undefined;
            this.xhr=this.log.fetch({error:this.handleLogError});
        },
        head:function(){
            this.log.offset=0;
            this.xhr=this.log.fetch({error:this.handleLogError});
        },
        handleTaskReset:function(){
            this.handleLogClear();
            this.fetchLog();
        },
        handleLogError:function(model, resp, options){
            if (resp.status == 400) {
                this.$el.find("pre#logContainer").html(resp.responseText);
            } else {
                this.$el.find("pre#logContainer").html("Error talking to minion");
            }
        },
        handleTaskStateChange:function(model){
            var label = this.$el.find("span#taskLabel");
            label.attr("class","label "+model.get("stateLabel"));
            label.html(model.get("stateText"));
        },
        handleHostNameChange:function(model){
            var host = this.$el.find("span#taskHost");
            host.html(model.get("host")+":"+model.get("hostPort"));
            this.log.host=model.get("host");
        },
        handleHostPortChange:function(model){
            this.handleHostNameChange(model);
            this.log.port=model.get("hostPort");
        },
        handleRunsAgoInputChange: function(event){
            var runsAgoInput = this.$el.find("input#runsAgoInput");
            var runsAgo = parseInt(runsAgoInput.val(),10);
            if(!_.isNaN(runsAgo)){
                if(!_.isEqual(this.runsAgo,runsAgo)){
                    this.log.clear();
                    this.runsAgo=runsAgo;
                    this.log.runsAgo=runsAgo;
                    runsAgoInput.val(this.runsAgo);
                    this.$el.find("pre#logContainer").html("loading...");
                    this.fetchLog();
                }
                this.saveState();
                return true;
            }
            else{
                event.preventDefault();
                event.stopImmediatePropagation();
                //TODO:show error message
                runsAgoInput.val(this.runsAgo);
                return false;
            }
        },
        handleLineInputChange: function(event){
            var lineInput = this.$el.find("input#linesInput");
            var lines = parseInt(lineInput.val(),10);
            if(!_.isNaN(lines) && lines>0){
                if(!_.isEqual(this.lines,lines)){
                    this.log.clear();
                    this.lines=lines;
                    this.log.lines=lines;
                    lineInput.val(this.lines);
                    this.$el.find("pre#logContainer").html("loading...");
                    this.fetchLog();
                }
                this.saveState();
                return true;
            }
            else{
                event.preventDefault();
                event.stopImmediatePropagation();
                //TODO:show error message
                lineInput.val(this.lines);
                return false;
            }
        },
        fetchLog:function(){
            if(_.isEqual(this.type,0)){//roll
                this.roll();
            }
            else if(_.isEqual(this.type,1)){//tail
                this.tail();
            }
            else if(_.isEqual(this.type,2)){
                this.head();
            }
        },
        handleLogControlClick:function(event){
            var button = $(event.currentTarget);
            var value = button.data("value");
            this.log.clear();
            this.type=value;
            this.fetchLog();
            this.saveState();
            this.$el.find("div.log-control-button button").removeClass("active");
            this.$el.find("div.log-control-button button[data-value='"+value+"']").addClass("active");
        },
        handleLogTypeClick:function(event){
            var button = $(event.currentTarget);
            this.$el.find("div.log-types button").removeClass("active");
            this.stdout = button.is("button#stdoutButton");
            this.log.stdout = this.stdout;
            if(this.stdout){
                this.$el.find("div.log-types button#stdoutButton").addClass("active");
            }
            else{
                this.$el.find("div.log-types button#stderrButton").addClass("active");
            }
            this.log.clear();
            this.fetchLog();
            this.saveState();
        },
        saveState:function(){
            var state = Cookies.getJSON("spawn") || {};
            state.log= {
                lines:this.lines,
                type:this.type,
                stdout:this.stdout,
                runsAgo:this.runsAgo
            };
            Cookies.set("spawn", state);
        },
        isLogAtBottom:function(){
            var pre = this.$el.find("pre#logContainer");
            var scrollTop = pre.scrollTop();
            var innerHeight = pre.innerHeight();
            var scrollHeight = pre.get(0).scrollHeight;
            var isBottom =((scrollTop+innerHeight+4)>=(scrollHeight));//for some reason, I'm finding that it could be off by 1 pixel
            return isBottom;
        },
        scrollLogToBottom:function(){
            var pre = this.$el.find("pre#logContainer");
            var scrollHeight = pre.get(0).scrollHeight;
            var innerHeight = pre.innerHeight();
            var deltaHeight = scrollHeight-innerHeight;
            if(deltaHeight>0){
                pre.scrollTop(scrollHeight-innerHeight);
            }
        },
        scrollLogToTop:function(){
            var pre = this.$el.find("pre#logContainer");
            pre.scrollTop(0);
        },
        remove:function(){
            this.handleLogClear();//to stop any outstanding timeouts
            Backbone.View.prototype.remove.apply(this,[]);
        }
    });
    return {
        Model:Model,
        Collection:Collection,
        TableView:TableView,
        TinyTableView:TinyTableView,
        DetailView:DetailView
    };
});
