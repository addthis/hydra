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
$.ajax({
	url: "/update/setup",
	type: "GET",
	global:true,
	success: function(data){
		Spawn.queueSize = data.spawnqueuesize;
		Spawn.jobCollection.reset(data.jobs);
		Spawn.hostCollection.reset(_.values(data.hosts));
		Spawn.macroCollection.reset(_.values(data.macros));
		Spawn.commandCollection.reset(_.values(data.commands));
		Spawn.aliasCollection.reset(_.values(data.aliases));
		Spawn.setQuiesce(data.quiesced=="1");
		Spawn.clientId = data.clientId;
		Backbone.history.start();
	},
	error: function(xhr, status, err){
	},
	dataType: "json"
});
Spawn = {};
Spawn.singletonAlerts={};
Spawn.updateSingleCategoryAlerts = function(category,log){
    if(category in Spawn.singletonAlerts){
        Spawn.singletonAlerts[category].close();
    }
    Spawn.singletonAlerts[category]=log;
};
Spawn.makeHtmlTitle = function(title){
    var hostname = location.hostname;
    var index = hostname.indexOf(".");
    if(index >= 0){
        hostname = hostname.substring(0, index);
    }
    return hostname + " " + title;
};
Spawn.currentView=null;
Spawn.showView = function(view,tab){
    if(!_.isNull(Spawn.currentView) && _.has("close",Spawn.currentView)){
        Spawn.currentView.close();
    }
    $("div.navbar div.navbar-inner ul.nav li").removeClass("active");
    $("div.navbar div.navbar-inner ul.nav li.dropdown ul.dropdown-menu li").removeClass("active");
    var tabItem=$("div.navbar div.navbar-inner ul.nav li a[href='#"+tab+"']").parent();
    tabItem.addClass("active");
    if(_.has(view,"div")){
        view.div.siblings().hide();
    }
    else{
        view.$el.siblings().hide();
    }
    view.render();
    Spawn.currentView=view;
};
$(document).ajaxError(function(xhr, status, err){
    if(status.status == 401){
        Alertify.dialog.prompt("Enter username:",function(str){
            $.cookie("username",{username:$.trim(str)},{expires:365});
            Spawn.init();
        });
    }
});
Spawn.init = function(){
    var username=$.cookie("username");
	Spawn.jobCollection = new Spawn.JobCollection([],{
		collectionName: "jobCollection"
	});
	Spawn.hostCollection = new Spawn.HostCollection([],{
		collectionName: "hostCollection"
	});
	Spawn.jobView = new Spawn.JobsView({
		collection:Spawn.jobCollection
		,hostCollection:Spawn.hostCollection
		,collectionName:"hostCollection"
	});
	Spawn.macroCollection = new Spawn.MacroCollection([],{
		collectionName:"macroCollection"
	});
	Spawn.macroView = new Spawn.MacrosView({
		collection:Spawn.macroCollection
	});
	Spawn.commandCollection = new Spawn.CommandCollection([],{
		collectionName:"commandCollection"
	});
	Spawn.commandView = new Spawn.CommandsView({
		collection:Spawn.commandCollection
	});
	Spawn.aliasCollection = new Spawn.AliasCollection([],{
		collectionName:"aliasCollection"
	});
	Spawn.aliasView = new Spawn.AliasesView({
		collection:Spawn.aliasCollection
	});
	Spawn.hostView = new Spawn.HostsView({
		collection: Spawn.hostCollection
		,jobCollection: Spawn.jobCollection
	});
	Spawn.settingView = new Spawn.SettingsView({
		model: new Spawn.GitModel()
	});
	Spawn.balanceParamView = new Spawn.BalanceParamView({
		model: new Spawn.BalanceParamModel()
	});
	$("li#quiesceButton").click(function(event){
		event.preventDefault();
		event.stopImmediatePropagation();
		Spawn.quiesceCluster();
	});
	$("a.brand").click(function(){//clicking on the brand is the same as clicking on the jobs tab
		$("a[href='#jobs']").click();
	});
	Spawn.router = new Spawn.Router();
    $.ajaxSetup({
        global:true,
        headers:{
            "Username":username.username
        }
    });
	Spawn.router.on("route:showIndex",function(){
		Spawn.router.navigate("jobs",{trigger:true});
	});
	Spawn.router.on("route:showJobsTable",function(){
		Spawn.showView(Spawn.jobView,"jobs");
		Spawn.jobView.model=undefined;
		Spawn.jobView.renderJobsTable();
		document.title=Spawn.makeHtmlTitle("Jobs");
	});
	Spawn.router.on("route:showExpNewJobConf",function(jobId){
		var isClone=!_.isUndefined(jobId);
		Spawn.showView(Spawn.jobView,"jobs");
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderExpandedConfiguration();
		Spawn.jobView.views.jobRightPanel.resizeElements();
		Spawn.jobView.model.fetchExpandedConfig();
		if(isClone){
			document.title=Spawn.makeHtmlTitle("Clone::"+jobId);
		}
		else{
			document.title=Spawn.makeHtmlTitle("New::Job");
		}
	});
	var showNewJob=function(jobId,view){
		Spawn.showView(Spawn.jobView,"jobs");
		var isClone=!_.isUndefined(jobId);
		var isNewAlreadyShowing=_.isUndefined(jobId) && !_.isUndefined(Spawn.jobView.model) && Spawn.jobView.model.isNew();
		var isCloneAlreadyShowing = (isClone && !_.isUndefined(Spawn.jobView.model) && !_.isUndefined(Spawn.jobView.model.cloneId) && _.isEqual(Spawn.jobView.model.cloneId,jobId));
		if(_.isUndefined(Spawn.jobView.model) || !isCloneAlreadyShowing || !Spawn.jobView.model.isNew()){
			if(!isClone && !isNewAlreadyShowing){
				Spawn.jobView.model=new Spawn.EditableJobModel();
				Spawn.jobView.model.cloneId=undefined;
				Spawn.jobView.closeJobDetailView();
			}
			else if(isClone && !isCloneAlreadyShowing){
				Spawn.jobView.model=new Spawn.EditableJobModel(
					Spawn.jobCollection.get(jobId).toJSON()
				);
				Spawn.jobView.model.cloneId=jobId;
				Spawn.jobView.closeJobDetailView();
			}
		}
		if(isClone && !isCloneAlreadyShowing){
			Spawn.jobView.model.fetch({
				success:function(model,response){
					model.set("config",response.config);
					model.set("parameters",response.parameters);
					model.set("description","CLONE "+response.description);
					model.unset("id");
					model.set("alerts",[]);
				}
			});
			Spawn.jobView.model.set("description","CLONE "+Spawn.jobView.model.get("description"));
		}
		Spawn.jobView.closeJobsTableView();
		if(view == "conf"){
			Spawn.jobView.renderJobDetailView().renderConfiguration();
		}
		else if(view == "alerts"){
			Spawn.jobView.renderJobDetailView().renderAlerts();
		}
		else if(view == "settings"){
			Spawn.jobView.renderJobDetailView().renderSettings();
		}
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(isClone){
			document.title=Spawn.makeHtmlTitle("Clone::"+jobId);
		}
		else{
			document.title=Spawn.makeHtmlTitle("New::Job");
		}
	};
	Spawn.router.on("route:showNewJobConf",function(jobId){
		showNewJob(jobId, "conf");
	});
	Spawn.router.on("route:showNewJobAlerts",function(jobId){
		showNewJob(jobId, "alerts");
	});
	Spawn.router.on("route:showNewJobSettings",function(jobId){
		showNewJob(jobId, "settings");
	});
	Spawn.router.on("route:showCloneJob",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		Spawn.jobView.loadDetailPanel(jobId,true);
		Spawn.jobView.loadDetailPanel();
		Spawn.jobView.closeQuickView();
		document.title=Spawn.makeHtmlTitle("Clone::"+jobId);
	});
	Spawn.router.on("route:showQuickJob",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		Spawn.jobView.model=Spawn.jobCollection.get(jobId);
		Spawn.jobView.renderJobsTable();
		Spawn.jobView.renderQuickJobView();
		document.title=Spawn.makeHtmlTitle("Quick::"+jobId);
	});
	Spawn.router.on("route:showQuickTask",function(jobId,taskNum){
		Spawn.showView(Spawn.jobView,"jobs");
		Spawn.jobView.model=Spawn.jobCollection.get(jobId);
		Spawn.jobView.renderJobsTable();
		Spawn.jobView.renderTaskView(taskNum);
		document.title=Spawn.makeHtmlTitle("Quick::"+jobId);
	});
	Spawn.router.on("route:showJobDetail",function(jobId){
		Spawn.router.navigate("jobs/"+jobId+"/conf",{trigger:true});
	});
	Spawn.router.on("route:showJobConfPanel",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
		if(!isAlreadyShowing){
			Spawn.jobView.model=new Spawn.EditableJobModel(
				Spawn.jobCollection.get(jobId).toJSON()
			);
			Spawn.jobView.model.cloneId=undefined;
			Spawn.jobView.closeJobDetailView();
		}
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderConfiguration();
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(!isAlreadyShowing){
			Spawn.jobView.model.fetch().always(function(){
				Spawn.jobView.model.trigger("reset");
			});
		}
		else{
			//TODO:yuck
			if(!Spawn.jobView.views.jobRightPanel.views.jobDetailView.views.currentTab.views.editor.hasReset){
				Spawn.jobView.views.jobRightPanel.views.jobDetailView.views.currentTab.views.editor.hasReset=true;
			}
		}
		document.title=Spawn.makeHtmlTitle("Job::"+jobId);
	});
	Spawn.router.on("route:showExpJobConfPanel",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
		if(!isAlreadyShowing){
			Spawn.jobView.model=new Spawn.EditableJobModel(
				Spawn.jobCollection.get(jobId).toJSON()
			);
			Spawn.jobView.model.cloneId=undefined;
			Spawn.jobView.closeJobDetailView();
		}
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderExpandedConfiguration();
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(!isAlreadyShowing){
			Spawn.jobView.model.fetch().always(function(){
				Spawn.jobView.model.fetchExpandedConfig();
			});
		}
		else{
			Spawn.jobView.model.fetchExpandedConfig();
		}
		document.title=Spawn.makeHtmlTitle("Job::"+jobId);
	});
	Spawn.router.on("route:showJobSettings",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
		if(!isAlreadyShowing){
			Spawn.jobView.model=new Spawn.EditableJobModel(
				Spawn.jobCollection.get(jobId).toJSON()
			);
			Spawn.jobView.closeJobDetailView();
		}
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderSettings();
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(!isAlreadyShowing){
			Spawn.jobView.model.fetch({reset:true});
		}
		document.title=Spawn.makeHtmlTitle("Job::"+jobId);
	});
    Spawn.router.on("route:showJobHistory",function(jobId){
        Spawn.showView(Spawn.jobView,"jobs");
        var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
        if(!isAlreadyShowing){
            Spawn.jobView.model=new Spawn.EditableJobModel(
                               Spawn.jobCollection.get(jobId).toJSON()
            );
            Spawn.jobView.closeJobDetailView();
        }
        Spawn.jobView.closeJobsTableView();
        Spawn.jobView.model.fetchHistory();
        Spawn.jobView.views.jobRightPanel.resizeElements();
        if(!isAlreadyShowing){
            Spawn.jobView.model.fetch({reset:true});
        }
        document.title=Spawn.makeHtmlTitle("Job::"+jobId);
    });
	Spawn.router.on("route:showJobTaskDetail",function(jobId,taskNum){
		Spawn.showView(Spawn.jobView,"jobs");
		var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
		if(!isAlreadyShowing){
			Spawn.jobView.model=new Spawn.EditableJobModel(
				Spawn.jobCollection.get(jobId).toJSON()
			);
			Spawn.jobView.closeJobDetailView();
		}
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderTaskDetail(taskNum);
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(!isAlreadyShowing){
			Spawn.jobView.model.fetch({reset:true});
		}
		document.title=Spawn.makeHtmlTitle("Job::"+jobId);
	});
	Spawn.router.on("route:showJobTaskPanel",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
		if(!isAlreadyShowing){
			Spawn.jobView.model=new Spawn.EditableJobModel(
				Spawn.jobCollection.get(jobId).toJSON()
			);
			Spawn.jobView.closeJobDetailView();
		}
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderTasks();
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(!isAlreadyShowing){
			Spawn.jobView.model.fetch({reset:true});
		}
		document.title=Spawn.makeHtmlTitle("Job::"+jobId);
	});
	Spawn.router.on("route:showJobAlerts",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
		if(!isAlreadyShowing){
			Spawn.jobView.model=new Spawn.EditableJobModel(
				Spawn.jobCollection.get(jobId).toJSON()
			);
			Spawn.jobView.closeJobDetailView();
		}
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderAlerts();
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(!isAlreadyShowing){
			Spawn.jobView.model.fetch({reset:true});
		}
		document.title=Spawn.makeHtmlTitle("Job::"+jobId);
	});
	Spawn.router.on("route:showJobDepPanel",function(jobId){
		Spawn.showView(Spawn.jobView,"jobs");
		var isAlreadyShowing = !_.isUndefined(Spawn.jobView.views.jobRightPanel.views.jobDetailView) && !_.isUndefined(Spawn.jobView.model) &&  _.isEqual(Spawn.jobView.model.id,jobId);
		if(!isAlreadyShowing){
			Spawn.jobView.model=new Spawn.EditableJobModel(
				Spawn.EditableJobModel.prototype.parse(Spawn.jobCollection.get(jobId).toJSON())
			);
			Spawn.jobView.closeJobDetailView();
		}
		Spawn.jobView.closeJobsTableView();
		Spawn.jobView.renderJobDetailView().renderDependencies();
		Spawn.jobView.views.jobRightPanel.resizeElements();
		if(!isAlreadyShowing){
			Spawn.jobView.model.fetch({reset:true});
		}
		document.title=Spawn.makeHtmlTitle("Job::"+jobId);
	});
	Spawn.router.on("route:showMacroTable",function(){
		Spawn.showView(Spawn.macroView,"macros");
		Spawn.macroView.render().renderMainView().renderTableView();
		Spawn.macroCollection.search().filter();
		document.title=Spawn.makeHtmlTitle("Macros");
	});
	Spawn.router.on("route:showNewMacro",function(){
		Spawn.showView(Spawn.macroView,"macros");
		var macroMainView = Spawn.macroView.render().renderMainView();
		macroMainView.model=new Spawn.MacroModel({
			owner:Spawn.user.username
		});
		macroMainView.renderDetailView();
		document.title=Spawn.makeHtmlTitle(name);
	});
	Spawn.router.on("route:showMacroDetail",function(name){
		Spawn.showView(Spawn.macroView,"macros");
		var macroMainView = Spawn.macroView.render().renderMainView();
		macroMainView.model=Spawn.macroCollection.get(name);
		macroMainView.renderDetailView();
		macroMainView.model.fetch();
		document.title=Spawn.makeHtmlTitle(name);
	});
	Spawn.router.on("route:showCommandTable",function(){
		Spawn.showView(Spawn.commandView,"commands");
		Spawn.commandView.render().renderMainView().renderTableView();
		Spawn.commandCollection.fetch({
			success:function(){
				Spawn.commandCollection.search().filter();
			}
		});
		document.title=Spawn.makeHtmlTitle("Commands");
	});
	Spawn.router.on("route:showNewCommand",function(name){
		Spawn.showView(Spawn.commandView,"commands");
		var mainView = Spawn.commandView.render().renderMainView();
		mainView.model=new Spawn.CommandModel({});
		mainView.renderDetailView();
		//mainView.model.fetch();
		document.title=Spawn.makeHtmlTitle("New Command");
	});
	Spawn.router.on("route:showCommandDetail",function(name){
		Spawn.showView(Spawn.commandView,"commands");
		var mainView = Spawn.commandView.render().renderMainView();
		mainView.model=Spawn.commandCollection.get(name);
		mainView.renderDetailView();
		if(!mainView.model.isNew()){
			mainView.model.fetch();
		}
		document.title=Spawn.makeHtmlTitle(name);
	});
	Spawn.router.on("route:showAliasTable",function(){
		Spawn.showView(Spawn.aliasView,"aliases");
		Spawn.aliasView.render().renderMainView().renderTableView();
		Spawn.aliasCollection.search().filter();
		document.title=Spawn.makeHtmlTitle("Aliases");
	});
	Spawn.router.on("route:showNewAlias",function(name){
		Spawn.showView(Spawn.aliasView,"aliases");
		var mainView = Spawn.aliasView.render().renderMainView();
		mainView.model=new Spawn.AliasModel({});
		mainView.renderDetailView();
		//mainView.model.fetch();
		document.title=Spawn.makeHtmlTitle("New Alias");
	});
	Spawn.router.on("route:showAliasDetail",function(name){
		Spawn.showView(Spawn.aliasView,"aliases");
		var mainView = Spawn.aliasView.render().renderMainView();
		mainView.model=Spawn.aliasCollection.get(name);
		mainView.renderDetailView();
		if(!mainView.model.isNew()){
			mainView.model.fetch();
		}
		document.title=Spawn.makeHtmlTitle(name);
	});
	Spawn.router.on("route:showHostGangliaGraphs",function(hostId){
		var model = Spawn.hostCollection.get(hostId);
		var view = new Spawn.GangliaGraphView({
			model: model
		}).render();
		Spawn.showView(view,"hosts");
		document.title=Spawn.makeHtmlTitle("Host Ganglia");
	});
	Spawn.router.on("route:showHostTable",function(){
		Spawn.showView(Spawn.hostView,"hosts");
		Spawn.hostView.render();
		Spawn.hostView.renderHostTable();
		Spawn.hostView.views.hostRightPanel.resizeElements();
		Spawn.hostCollection.search().filter();
		document.title=Spawn.makeHtmlTitle("Hosts");
	});
	Spawn.router.on("route:showHostDetail",function(name){
		Spawn.showView(Spawn.hostView,"hosts");
		Spawn.hostView.render();
		var hostModel = Spawn.hostCollection.get(name);
		if(!_.isUndefined(hostModel)){
			Spawn.hostView.model=hostModel;
			Spawn.hostView.renderHostDetail();
		}
		else{
			Spawn.router.navigate("hosts",{trigger:true});
			Alertify.log.error("Host: "+name+" was not found.");
		}
		document.title=Spawn.makeHtmlTitle(name);
	});
	Spawn.router.on("route:showSettings",function(){
		Spawn.showView(Spawn.settingView,"settings");
		Spawn.settingView.model.fetch();
		$("div.navbar div.navbar-inner ul.nav li.dropdown").addClass("active");
		document.title=Spawn.makeHtmlTitle("Settings");
	});
	Spawn.router.on("route:showBalanceParams",function(){
		Spawn.showView(Spawn.balanceParamView,"balanceParams");
		Spawn.balanceParamView.model.fetch();
		$("div.navbar div.navbar-inner ul.nav li.dropdown").addClass("active");
		document.title=Spawn.makeHtmlTitle("Spawn:: Balance Parameters");
	});
	Spawn.server.on("host.delete",function(message){
		var host = Spawn.hostCollection.get(message.uuid);
		if(!_.isUndefined(host)){
			Spawn.hostCollection.remove(host);
		}
	});
	Spawn.server.on("cluster.quiesce",function(message){
		var isQuiesced = Boolean(message.quiesced);
		if(isQuiesced){
			Alertify.log.info("Cluster has been quiesced by "+message.username);
		}
		else{
			Alertify.log.info("Cluster has been reactivatd by "+message.username);
		}
		Spawn.setQuiesce(message.quiesced);
	});
	Spawn.loadUser(username,$("div#userDiv"));
	Spawn.server.connect();
};

Spawn.setQuiesce = function(q){
	Spawn.isQuiesced=q;
	if(q){
		$("li#quiesceButton a").text("Reactivate");
		$("div#navbarInfo").append("<span id='quiescedLabel' style='margin-left:100px;' class='label label-warning'>CLUSTER QUIESCED</span>");
	}
	else{
		$("li#quiesceButton a").text("Quiesce");
		$("div#navbarInfo span#quiescedLabel").remove();
	}
};
Spawn.quiesceCluster = function(){
    Alertify.dialog.confirm( ((Spawn.isQuiesced?"un":"")+"quiesce the cluster? (if you don't know what you're doing, hit cancel!)"), function (e) {
        if (_.isUndefined(e)) {
            $.ajax({
                url: "/update/quiesce",
                type: "GET",
                data: {quiesce:(Spawn.isQuiesced?"0":"1")},
                success: function(data){
                    Alertify.log.info("Cluster "+(data.quiesced=="1"?"quiesced":"reactivated")+" successfully.");
                },
                error: function(){
                    Alertify.dialog.alert("You do not have sufficient privileges to quisce cluster");
                },
                dataType: "json"
            });
        } else {
            console.log("Quiesce has been canceled.");
        }
    });
};
Spawn.loadUser = function(user,div){
	Spawn.user = user;
	this.div=div;
	this.div.css("padding","10px 15px 10px");
	this.div.empty();
	var name=user.username;
	if(user.isAdmin){
		name+=" (admin)";
	}
	this.div.append($("<strong>"+name+"</strong>"));
};
Spawn.SettingsView = Backbone.View.extend({
    el:"div#settings",
    className:"spawn12",
    events:{
    },
    template:_.template($("#settingsTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.$el=$(this.el);
        this.listenTo(this.model,"change",this.render);
    },
    render:function(){
        this.$el.empty();
        var html = this.template(this.model.toJSON());
        this.$el.html(html);
        this.show();
        return this;
    },
    close:function(){
        this.$el.remove();
        return this;
    },
    show:function(){
        this.$el.show();
    },
    hide:function(){
        this.$el.hide();
        return this;
    }
});
Spawn.SidebarView= Backbone.View.extend({
    tagName: "ul",
    className: "nav nav-list",
    events: {
    },
    initialize: function(options) {
        //console.log("initializing sidebar");
        _.bindAll(this);
        this.template=options.template;
        if(options.key){
            this.$el.data("key",options.key);
        }
        this.listenTo(this.collection,"app:filtered",this.handleFiltered);
        this.listenTo(this.collection,"app:clearFilter",this.handleClearFilter);
        //this.collection.search();
    },
    render: function() {
        this.$el=$(this.el);
        var html = this.template(this.model);
        this.$el.html(html);
        return this;
    },
    handleFiltered:function(criteria){
        var key = this.$el.data("key");
        var value = criteria[key];
        if(!_.isUndefined(key)){
            this.$el.find("li").removeClass("active");
            if(!_.isEqual("*",key) && !_.isUndefined(value)){
                var item = this.$el.find("a[data-"+key+"="+value+"]").parent();
                item.addClass("active");
            }
            else{
            }
        }
    },
    handleClearFilter:function(){
        this.$el.find("li").removeClass("active");
    }
});
Spawn.server= _.extend({
	connect:function(){
		_.bindAll(this);
		this.ws=new WebSocket("ws://"+window.location.host+"/ws?user="+Spawn.user.username);//+"&session="+this.user.get("session"));

		this.ws.onopen=this.handleOpen;
		this.ws.onclose=this.handleClose;
		this.ws.onmessage=this.handleMessage;
		this.heartbeat=undefined;
		//console.log(templText);
		return this;
	},
	user:"anonymous",
	sendText:function(text){
		this.ws.send(text);
		return this;
	},
	sendJSON: function(data){
		var stringified = JSON.stringify(data);
		//console.log("Sending JSON: "+stringified);
		this.ws.send(stringified);
		return this;
	},
	handleOpen:function(event){
		console.log("Connection with MQMaster has been established.");
		this.trigger("open");
		this.startHeartbeat();
	},
	handleClose:function(event){
		console.log("Closing connection... code: "+event.code+", reason: "+event.reason+", wasClean: "+event.wasClean);
		this.trigger("close");
		this.stopHeartbeat();
	},
	handleMessage:function(event){
		//console.log("[Message Received] "+event.data);
		if(!_.isEqual(event.data,"pong")){
			var data = JSON.parse(event.data);
			var message = JSON.parse(data.message);
			if(_.isEqual(data.topic,"event.batch.update")){
				var count = {},self=this;
				_.each(message,function(ev){
					self.trigger(ev.topic,ev.message);
					if(_.has(count,ev.topic)){
						count[ev.topic]=count[ev.topic]+1;
					}
					else{
						count[ev.topic]=1;
					}
					//console.log(ev);
				});
			}
			else{
				this.trigger(data.topic,message);
				console.log(data.topic);
			}
		}
	},
	stopHeartbeat:function(){
		if(!_.isUndefined(this.heartbeat)){
			clearInterval(this.heartbeat);
			this.heartbeat=undefined;
		}
	},
	startHeartbeat:function(){
		if(_.isUndefined(this.heartbeat)){
			var self=this;
			this.heartbeat = setInterval(function(){
				self.sendText("ping");
			},10000);
		}
	}
}, Backbone.Events);
Spawn.SelectableModel = Backbone.Model.extend({
    initialize:function(){
        this.isSelected=false;
        this.isHidden=false;
    },
    defaults: {
        description:"(no title)"
    },
    select:function(){
        this.isSelected=true;
        this.trigger("app:select",this);
    },
    unselect:function(){
        this.isSelected=false;
        this.trigger("app:unselect",this);
    },
    contains:function(opts){
        if(_.isEmpty(opts) || _.isEmpty(opts.query) || _.isEmpty(opts.keys)){
            return true;
        }
        var searchVals = _.values(_.pick(this.toJSON(),opts.keys));
        var isPresent = _.some(searchVals,function(val){
            var testVal = (_.isArray(val) || _.isObject(val)?JSON.stringify(val):val);
            return testVal.toString().indexOf(opts.query)>-1;
        });
        return isPresent;
    },
    //returns true to keep, or false to filter out (hide)
    filter:function(criteria){
        var keys = _.keys(criteria);
        var self=this;
        var keep = false;
        _.every(keys,function(key){
            keep = _.isEqual(self.get(key),criteria[key]);
            return keep;
        });
        return keep;
    }
});
Spawn.LogModel = Backbone.View.extend({
    initialize:function(options){
        var task = options.task;
        //console.log("Initializing log for task "+task.id+" and job "+task.get("jobUuid"));
    }
});
Spawn.TaskModel = Spawn.SelectableModel.extend({
    initialize:function(){
        //console.log("Initializing task model.");
        this.isSelected=false;
        this.isHidden=false;
        this.tailReqId=null;
    },
    idAttribute:"node",
    handleUpdate:function(data){
        this.set(data);
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
            success: function(data){
                if(notify){
                    Alertify.log.info("Task "+self.id+" stopped.");
                }
            },
            error: function(e){
                Alertify.log.error("Error stopped task: "+self.id+" <br/>"+e.responseText);
            },
            dataType: "json"
        });
    },
    kick:function(notify){
        notify = notify || false;
        var self = this;
        var performkick=function() {
         $.ajax({
                    url: "/task/start",
                    data:{
                        job:this.get("jobUuid"),
                        task:this.get("node")
                    },
                    type: "GET",
                    success: function(data){
                        if(notify){
                            Alertify.log.info("Task "+self.id+" kicked.");
                        }
                    },
                    error: function(e){
                        Alertify.log.error("Error kicking task: "+self.id+" <br/>"+e.responseText);
                    },
                    dataType: "json"
                });
        };
        var confirmkick=function() {
            Alertify.dialog.confirm(
                "The cluster is quiesced. Are you sure you want to rekick the task?",
                function (e) {
                    performkick()
                },
                function(e)
                {
                    Alertify.log.info("Task "+self.id+" kick canceled.");
                });
        };
        if (Spawn.isQuiesced) {
            confirmkick();
        } else {
            performkick();
        }
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
            success: function(data){
                if(notify){
                    Alertify.log.info("Task "+self.id+" killed.");
                }
            },
            error: function(e){
                Alertify.log.error("Error killing task: "+self.id+" <br/>"+e.responseText);
            },
            dataType: "json"
        });
    },
    tail: function(opts){
        this.set({"log":undefined},{silent:true});//to signal loading..
        this.trigger("change:log",this);
        var params = _.extend({
            id:this.get("jobUuid"),
            node:this.get("node"),
            out:1
        },opts);
        var self=this;
        var tailReqId = _.uniqueId(this.get("jobUuid")+"|"+this.get("node"));
        this.tailReqId=tailReqId;
		var t =$.getJSONP({
			url: "http://"+this.get("hostUrl")+"/job.log",
			type: "GET",
            data:params,
			success: function(data){
                if(_.isEqual(self.tailReqId,tailReqId)){
                    self.set({"log":data},{silent:true});
                    self.trigger("change:log",self);
                }
            },
			error: function(e){
                if(_.isEqual(self.tailReqId,tailReqId)){
                    self.set({"log":null},{silent:true});
                    self.trigger("change:log",self);
                }
			},
			dataType: "jsonp"
		});
    },
    roll: function(opts){
        var params = _.extend({
            id:this.get("jobUuid"),
            node:this.get("node"),
            out:1
        },opts);
        var self=this;
        var tailReqId = _.uniqueId(this.get("jobUuid")+"|"+this.get("node"));
        this.tailReqId=tailReqId;
		var t =$.getJSONP({
			url: "http://"+this.get("hostUrl")+"/job.log",
			type: "GET",
            data:params,
			success: function(data){
                if(_.isEqual(self.tailReqId,tailReqId)){
                    //console.log(data);
                    self.set({"log":data},{silent:true});//TODO: stdout or stderr? check opts to find which one
                    self.trigger("change:log",self);
                }
            },
			error: function(e){
                if(_.isEqual(self.tailReqId,tailReqId)){
                    self.set({"log":null},{silent:true});//TODO: stdout or stderr? check opts to find which one
                    self.trigger("change:log",self);
                }
			},
			dataType: "jsonp"
		});
    },
    head:function(opts){
        //this.set({"log":undefined},{silent:true});//to signal loading..
        //this.trigger("change:log",this);
        var params = _.extend({
            id:this.get("jobUuid"),
            node:this.get("node"),
            out:1
        },opts);
        params.offset=0;//head must enforce this condition
        var self=this;
        var tailReqId = _.uniqueId(this.get("jobUuid")+"|"+this.get("node"));
        this.tailReqId=tailReqId;
		var t =$.getJSONP({
			url: "http://"+this.get("hostUrl")+"/job.log",
			type: "GET",
            data:params,
			success: function(data){
                if(_.isEqual(self.tailReqId,tailReqId)){
                    //console.log(data);
                    self.set({"log":data},{silent:true});//TODO: stdout or stderr? check opts to find which one
                    self.trigger("change:log",self);
                }
            },
			error: function(e){
                if(_.isEqual(self.tailReqId,tailReqId)){
                    self.set({"log":null},{silent:true});//TODO: stdout or stderr? check opts to find which one
                    self.trigger("change:log",self);
                }
			},
			dataType: "jsonp"
		});
    }
});
Spawn.SelectableCollection = Backbone.Collection.extend({
	initialize:function(opts){
		opts = opts || {};
		this.collectionName = this.collectionName || opts.collectionName || "selectableCollection";
        if(!_.isUndefined(this.collectionName)){
			var cookie = $.cookie(this.collectionName);
            if(!_.isUndefined(cookie)){
				this.search(cookie.searchQuery);
				this.filter(cookie.criteria);
			}
		}
	},
    clear:function(){
        this.criteria={},this.searchQuery="";
        _.map(this.models,function(model){
            model.isHidden=false;
            model.trigger("app:show",model);
        });
        this.trigger("app:clearFilter");
        this.persistToCookie();
    },
    persistToCookie:function(){
        if(!_.isUndefined(this.collectionName)){
			$.cookie(this.collectionName,{
				criteria: this.criteria,
				searchQuery: this.searchQuery
			},{
				expires:365
			});
        }
    },
    clearFilter:function(){
        this.criteria={};
        this.trigger("app:clearFilter");
        this.persistToCookie();
    },
    selectAll: function(){
        _.map(this.models,function(model){
            if(!model.isHidden){
                model.select();
            }
        });
        this.trigger("app:selectAll");
    },
    unselectAll: function(){
        _.map(this.models,function(model){
            if(!model.isHidden){
                model.unselect();
            }
        });
        this.trigger("app:unselectAll");
    },
    filter:function(criteria){
        this.criteria=criteria || this.criteria;
        if(_.isEmpty(this.criteria)){
            this.search();
        }
        else{
            var keys = _.keys(this.criteria);
            var self = this;
            _.map(this.models,function(model){
                var keep = model.filter(self.criteria) && model.contains(self.searchQuery);
                if(keep){
                    model.isHidden=false;
                    model.trigger("app:show",model);
                }
                else{
                    model.isHidden=true;
                    model.trigger("app:hide",model);
                }
            });
        }
        this.persistToCookie();
        this.trigger("app:filtered",this.criteria);
        return this;
    },
    search:function(opts){
        this.searchQuery=opts || this.searchQuery;
        var self=this;
        _.map(this.models,function(model){
            var isPresent = model.contains(self.searchQuery);
            if(isPresent){
                model.isHidden=false;
                model.trigger("app:show",model);
            }
            else{
                model.isHidden=true;
                model.trigger("app:hide",model);
            }
        });
        this.persistToCookie();
        this.trigger("app:clearFilter");
        return this;
    }
    ,handleChange:function(model){
        var show=true;
        if(!_.isEmpty(this.criteria)){
            show = model.filter(this.criteria);
        }
        if(show && !_.isEmpty(this.searchQuery)){
            show = model.contains(this.searchQuery);
        }
        if(show){
            model.trigger("app:show",model);
        }
        else{
            model.trigger("app:hide",model);
        }
    }
    ,getSelectedIds:function(){
        var selectedIds=[];
        this.each(function(model){
            if(model.isSelected){
                selectedIds.push(model.id);
            }
        });
        return selectedIds;
    }
    ,getSelectedIdMap:function(){
        var selectedMap={};
        this.each(function(model){
            if(model.isSelected){
                selectedMap[model.id]=0;
            }
        });
        return selectedMap;
    },
    sort:function(){
        return Backbone.Collection.prototype.sort.apply(this);
    }
});
Spawn.TaskCollection = Spawn.SelectableCollection.extend({
    initialize:function(opts){
        this.url="/job/get?id="+opts.jobId+"&field=nodes";
        this.jobId=opts.jobId;
        this.idAttribute="node";
        this.listenTo(Spawn.server,'job.update',this.handleJobUpdate);
        this.collectionName="taskCollection";
        Spawn.SelectableCollection.prototype.initialize.apply(this,[opts]);
    },
    parse:function(tasks){
        _.each(tasks,function(task){
            var hostObj = Spawn.hostCollection.get(task.hostUuid);
            task.host = (!_.isUndefined(hostObj)?hostObj.get("host"):"N/A");
            task.hostUrl = (!_.isUndefined(hostObj)?hostObj.get("host")+":"+hostObj.get("port"):undefined);
        });
        return tasks;
    },
    model: Spawn.TaskModel,
    handleJobUpdate:function(jobData){
        if(_.isEqual(jobData.id,this.jobId)){
            var self=this;
            $.ajax({
                type: "GET",
                url:"/job/get",
                data:{
                    id:jobData.id,
                    field:"nodes"
                },
                success: function(data){
                    //self.reset(data,{change:true});
                    _.each(data,function(task){
                        self.get(task.node.toString()).set(task);
                    });
                },
                error: function(e){
                    console.log("Error fetching tasks for "+jobData.id);
                },
                dataType: "json"
            });
        }
    }
});
Spawn.HostModel = Spawn.SelectableModel.extend({
    initialize:function(){
        //console.log("Initializing host model.");
        //this.listenTo(Spawn.updater,'host.update',this.handleHostUpdate);
        this.isSelected=false;
        this.isHidden=false;
    }
    ,idAttribute:"uuid"
    ,url:function(){
        return "/host/get?id="+this.id;
    }
    ,defaults: {
        diskUsed:0,
        diskMax:0,
        used:{disk:0},
        max:{disk:0}
    }
    ,parse:function(host){
        if(!_.isEmpty(host.used)){
            host.diskUsed=host.used.disk;
        }
        if(!_.isEmpty(host.max)){
            host.diskMax=host.max.disk;
        }
        if(!_.isEmpty(host.score)){
            host.score=Math.round(host.score);
        }
        return host;
    }
    ,handleHostUpdate: function(data){
        //if(_.isEqual(this.id,data.uuid)){
            //this.set(data);
            //console.log(data);
            //console.log("Host model has changed.");
        //}
    }
});
Spawn.HostCollection = Spawn.SelectableCollection.extend({
    initialize:function(opts){
        this.url="/host/list";
		opts = opts || {};
		this.collectionName = opts.collectionName || "hostCollection";
		Spawn.SelectableCollection.prototype.initialize.apply(this,[opts]);
        this.listenTo(Spawn.server,'host.update',this.handleHostUpdate);
        this.listenTo(Spawn.server,'host.delete',this.handleHostDelete);
    }
    ,model: Spawn.HostModel
    ,idAttribute:"uuid"
    ,handleHostUpdate:function(data){
        var model = this.get(data.uuid);
        if(!_.isUndefined(model)){
            model.set(data);
            //this.set([model]);
        }
    }
    ,handleHostDelete:function(data){
        var host = this.get(data.uuid);
        if(!_.isUndefined(host)){
            this.remove(host);
        }
    }
    ,rebalanceBatch:function(hostIds){
        var count = hostIds.length;
        $.ajax({
            url: "/host/rebalance",
            type: "get",
            data:{id:hostIds.join(",")},
            statusCode: {
                500: function(data) {
                    Alertify.log.error(data.responseText,15000);
                },
                200: function(data){
                    var count = data.length;
                    Alertify.log.success(count+" host(s) rebalanced",2000);
                }
            },
            datatype: "json"
        });
    }
    ,dropBatch:function(hostIds){
        var count = hostIds.length;
        $.ajax({
            url: "/host/drop",
            type: "get",
            data:{id:hostIds.join(",")},
            statusCode: {
                500: function(data) {
                    Alertify.log.error(data.responseText,15000);
                },
                200: function(data){
                    Alertify.log.success(count+" host(s) dropped successfully",2000);
                }
            },
            datatype: "json"
        });
    }
    ,failBatch:function(hostIds){
        var count = hostIds.length;
        $.ajax({
            url: "/host/fail",
            type: "get",
            data:{id:hostIds.join(",")},
            statusCode: {
                500: function(data) {
                    Alertify.log.error(data.responseText,15000);
                },
                200: function(data){
                    if(data.success>0){
                        Alertify.log.success(data.success+" host(s) failed successfully",2000);
                    }
                    if(data.error>0){
                        Alertify.log.error(data.error+" host(s) did not fail correctly",15000);
                    }
                }
            },
            datatype: "json"
        });
    }
    ,toggleBatchHosts:function(hostIds, dis){
        var count = hostIds.length;
        $.ajax({
            url: "/host/toggle",
            type: "get",
            data:{hosts:hostIds.join(","), disable:dis},
            statusCode: {
                500: function(data) {
                    Alertify.log.error(data.responseText,15000);
                },
                200: function(data){
                    Alertify.log.success(count+" host(s) "+(dis?"dis":"en")+"abled successfully",2000);
                    Spawn.hostCollection.fetch();
                }
            },
            datatype: "json"
        });
    }
});
Spawn.JobModel = Spawn.SelectableModel.extend({
    initialize:function(){
        //console.log("Initializing job model.");
        this.listenTo(Spawn.server,'job.update',this.handleJobUpdate);
        this.isSelected=false;
        this.isHidden=false;
        //this.url="/job/get?id="+this.id;
        this.saveUrl="/job/submit";
    },
    url:function(){
        return "/job/get?id="+this.id;
    },
    defaults: {
        maxRunTime:""
        ,rekickTimeout:""
        ,priority:0
        ,nodes:1
        ,queryConfig:{
            canQuery: true,
            consecutiveFailureThreshold: "",
            queryTraceLevel: ""
        }
        ,commit:""
        ,config:""
        ,expandedConfig:""
        ,history:[]
        ,parameters:[]
        ,alerts:[]
        ,hourlyBackups:1
        ,dailyBackups:1
        ,weeklyBackups:1
        ,monthlyBackups:1
        ,maxSimulRunning:0
        ,minionType:"default"
        ,replicas:1
        ,readOnlyReplicas:0
        ,dontAutoBalanceMe:true
        ,command:"default-task"
        ,running:0
        ,errored:0
        ,done:0
        ,state:0
        ,newJob:true
        ,description:"(no title)"
    },
    parse:function(job){
        if(_.isArray(job.nodes)){
            job.nodes=job.nodes.length;
        }
        if(_.isArray(job.readOnlyReplicas)){
            job.nodes=job.readOnlyReplicas.length;
        }
        if(_.isEmpty(job.queryConfig)){
            job.queryConfig={
                canQuery: false,
                consecutiveFailureThreshold: "",
                queryTraceLevel: ""
            };
        }
        job.canQuery=job.queryConfig.canQuery;
        job.consecutiveFailureThreshold=job.queryConfig.consecutiveFailureThreshold;
        job.queryTraceLevel=job.queryConfig.queryTraceLevel;
        return job;
    },
    getTaskCollection:function(){
        return new Spawn.TaskCollection({
            jobId:this.id,
            idAttribute: "node",
            opts:"taskCollection"
        });
    },
    rebalance:function(tasks){
        var self=this;
        $.ajax({
            url: "/job/rebalance",
            type: "GET",
            data:{id:this.id, tasksToMove:tasks},
            statusCode: {
                500: function(data) {
                    Alertify.log.error(e.responseText,5000);
                },
                200: function(data){
                    Alertify.log.success(data.responseText,2000);
                }
            },
            dataType: "json"
        });
    },
    enable:function(){
        var self=this;
        $.ajax({
            url: "/job/enable?jobs="+self.id+"&enable=1",
            type: "GET",
            success: function(data){
                //Alertify.log.success(jobid+" enabled",2000);
                Alertify.log.info(self.id+" job enabled.",2000)
            },
            error: function(e){
                Alertify.log.error("Error enabling job "+self.id);
                throw new Error(e.error());
            },
            dataType: "json"
        });
    },
    disable:function(){
        var self=this;
        $.ajax({
            url: "/job/enable?jobs="+self.id+"&enable=0",
            type: "GET",
            success: function(data){
                Alertify.log.info(self.id+" job disabled",2000);
            },
            error: function(e){
                Alertify.log.error("Error disabling job "+self.id);
                throw new Error(e.error());
            },
            dataType: "json"
        });
    },
    checkDirs:function(){
        var self=this;
		$.ajax({
			url: "/job/checkJobDirs",
			type: "GET",
			data: {id:self.id},
			success: function(data){
                var checkDirsModal = new CheckDirsModalView({
                    job:self.toJSON(),
                    matches:data
                });
                checkDirsModal.render();
			},
			error: function(e){
				throw new Error(e.error());
			},
			dataType: "json"
		});
    },
    fixDirs:function(node){
        var self=this;
        node = node || -1;
		$.ajax({
			url: "/job/fixJobDirs",
			type: "GET",
			data: {
                id:self.id,
                node:node
            },
			success: function(data){
                Alertify.log.success(data);
			},
			error: function(e){
				throw new Error(e.error());
			},
			dataType: "text"
		});
    },
    expand:function(){
        window.location.href="/job/expand?id="+this.id;
    },
    fetchConfig:function(){
        var self=this;
        $.ajax({
            url: "/job/get",
            type: "GET",
            data: {
                id:this.id,
                field:"config"
            },
            statusCode: {
                500: function(res){
                    Alertify.log.error(res.responseText,15000);
                },
                200: function(data){
                    self.set("config",data);
                }
            }
        });
    },
    listBackups:function(node){
        var self=this;
        $.ajax({
            url: "/job/backups.list",
            data: {
                id:this.id,
                node:(_.isUndefined(node)?-1:node)
            },
            statusCode: {
                404: function() {
                    Alertify.log.info("Job with id "+self.id+" was not found.");
                },
                500: function(res){
                    Alertify.log.error("Error getting backups for "+self.id+":\n"+res.responseText,15000);
                },
                503: function(res){
                    Alertify.log.error(res.responseText,15000);
                },
                    $(_.template($("#jobRevertModalTemplate").html())({
                        job:self,
                        backups:data,
                        nodeNum:-1
                    })).modal("show");
                }
            },
            dataType: "json"
        });
    },
    query:function(){
        window.open("http://"+document.domain+":2222/query/index.html?job="+this.id,"_blank");
    },
    validateSource:function(){
                     id:self.id,
                     config:self.get("config")
                 };
        _.each(this.get("parameters"),function(param){
            data["sp_"+param.name]=param.value;
        });
        $.ajax({
            url: "/job/validate",
            type: "POST",
            data: data,
            success: function(data){
                var log;
                if(data.result=="preExpansionError"){
                    log=Alertify.log.error(data.message, 60000);
                }
                else if(data.result=="postExpansionError"){
                    log=Alertify.log.error(data.message, 60000);
                    if (!_.isUndefined(self.cloneId)){
                        Spawn.router.navigate("#jobs/"+self.cloneId+"/clone/expandConf",{trigger:true});
                    }
                    else if(_.isUndefined(self.id)){
                        Spawn.router.navigate("#jobs/new/expandConf",{trigger:true});
                    }
                    else{
                        Spawn.router.navigate("#jobs/"+self.id+"/expandConf",{trigger:true});
                    }
                }
                else{
                    log=Alertify.log.success("Job is valid.");
                }
                Spawn.updateSingleCategoryAlerts('validation', log)
            },
            error: function(){
                Alertify.log.error("Error requesting job validation.");
            },
            dataType: "json"
        });
    },
    delete:function(dontShowSuccessAlert){
        var self=this;
        $.ajax({
            url: "/job/delete",
            type: "GET",
            data: {id:self.id},
            statusCode: {
                404: function() {
                    Alertify.log.info("Job with id "+self.id+" was not found.");
                },
                500: function(res){
                    Alertify.log.error("Error deleting job "+self.id+":\n"+res.responseText);
                },
                200: function(){
                    if(!dontShowSuccessAlert){
                        Alertify.log.success("Job deleted successfully.");
                    }
                    //delete Spawn.jobs[self.id];
                    Spawn.jobCollection.remove(self);//TODO: global Spawn.jobCollection should not be accessed here
                    Spawn.router.navigate("#jobs",{trigger:true});
                }
            },
            dataType: "text"
        });
    },
    kick:function(){
        var self=this;
        var performkick=function() {
            $.ajax({
                url: "/job/start?jobid="+self.id,
                type: "GET",
                success: function(data){
                    Alertify.log.info(self.id+" job kicked.",2000)
                },
                error: function(e){
                    Alertify.log.error("Error kicking: "+self.id+". <br/> "+e.responseText);
                },
                dataType: "json"
            });
        };
        var confirmkick=function() {
            Alertify.dialog.confirm(
               "The cluster is quiesced. Are you sure you want to rekick the job?",
               function (e) {
                   performkick()
               },
               function(e)
               {
                   Alertify.log.info("Job "+self.id+" kick canceled.");
               });
        };
        if (Spawn.isQuiesced) {
            confirmkick();
        } else {
            performkick();
        }
    },
    stop:function(){
        var self=this;
		$.ajax({
			url: "/job/stop?jobid="+self.id,
			type: "GET",
			success: function(data){
                Alertify.log.info(self.id+" job stopped.",2000)
            },
			error: function(e){
                Alertify.log.error("Error stopping: "+self.id+". <br/> "+e.responseText);
			},
			dataType: "json"
		});
    },
    kill:function(){
        var self=this;
		$.ajax({
			url: "/job/stop?jobid="+self.id+"&force=true",
			type: "GET",
			success: function(data){
                Alertify.log.info(self.id+" job killed.",2000)
            },
			error: function(e){
                Alertify.log.error("Error killing: "+self.id+". <br/> "+e.responseText);
			},
			dataType: "json"
		});
    }
});
Spawn.EditableJobModel = Spawn.JobModel.extend({
    initialize:function(){
        //console.log("Initializing job model.");
        this.listenTo(Spawn.server,'job.update',this.handleJobUpdate);
        this.isSelected=false;
        this.isHidden=false;
        this.saveUrl="/job/submit";
        this.userEditted=false;
    },
    handleJobUpdate: function(jobData){
        if(_.isEqual(this.get("id"),jobData.id)){
            //this.set(this.parse(_.omit(jobData,"config","parameters")));
            this.set({
                state: jobData.state
            });
        }
    },
    save:function(fields){
        var self = this;
        fields = fields || ["id","nodes","description","config","maxRunTime","rekickTimeout","priority","command","hourlyBackups","dailyBackups","weeklyBackups","monthlyBackups","minionType","replicas","readOnlyReplicas","dontAutoBalanceMe","maxSimulRunning","alerts"];
        var data =_.pick(self.toJSON(),fields);
        data.spawn=0;
        data.manual=1;
        data.commit=this.commit;
        _.each(this.get("parameters"),function(param){
            data["sp_"+param.name]=param.value;
        });
        var queryConfig = this.get("queryConfig");
        _.each(_.keys(queryConfig),function(qcKey){
            data["qc_"+qcKey]=queryConfig[qcKey];
        });
        data.alerts=[];
        _.each(this.get("alerts"),function(alrt){
            if(!_.isEmpty(alrt.email) && _.isNumber(parseInt(alrt.timeout))){
                data.alerts.push(alrt);
            }
        });
        data.alerts=JSON.stringify(data.alerts);
        $.ajax({
            url: self.saveUrl,
            type: "POST",
            data: data,
            success: function(data){
                //alert(JSON.stringify(data));
            },
            error:function(resp){
                //alert(resp.responseText);
            },
            statusCode: {
                404: function(data) {
                    Alertify.log.info(data.responseText);
                    Spawn.router.navigate("jobs",{trigger:true});
                },
                500: function(data) {
                    Alertify.log.error("Error saving job: \n\n"+data.responseText);
                },
                200: function(data){
                    if(self.isNew()){
                       self.set({
                           "id":data.id
                       },{silent:true});
                       self.fetch({
                           error:function(){
                               Alertify.log.error("Job was created with id: "+data.id+" but there was an error fetching it.");
                           },
                           success:function(){
                                //Spawn.jobCollection.add(self,{trigger:true});
								var hash = window.location.hash;
								var index = hash.lastIndexOf("/");
								var tab = hash.substring(index+1);
								tab = (_.isUndefined(tab)?"conf":tab);
								Alertify.log.success("Job created successfully.");
								Spawn.router.navigate("#jobs/"+data.id+"/"+tab,{trigger:true});
                           }
                       });
                    }
                    else{
                        Alertify.log.success("Job saved successfully.");
                    }
                    self.userEditted=false;
                }
            }
        });
    },
    fetchExpandedConfig:function(){
        var self=this;
        var data={id:self.id,config:self.get("config")};
        _.each(this.get("parameters"),function(param){
            data["sp_"+param.name]=param.value;
        });
        $.ajax({
            url: "/job/expand",
            type: "POST",
            data: data,
            statusCode: {
                500: function(res){
                    Alertify.log.error(res.responseText,15000);
                },
                200: function(data){
                    self.set("expandedConfig",data);
                    //self.trigger("reset");
                }
            }
        });
    },
    fetchHistory:function(){
    	var self=this;
    	$.ajax({
    		url: "/job/history",
    		type: "GET",
			data: {id:self.id},
			success: function(data){
                Spawn.jobView.renderJobDetailView().renderHistory(data);
			},
			error: function(e){
				throw new Error(e.error());
			},
			dataType: "json"
    	})
    },
    runConfigCommand:function(endpoint, jobId, commit, replaceConfig){
    	var self=this;
    	$.ajax({
    		url: "/job/" + endpoint,
    		type: "GET",
			data: {id:jobId, commit:commit},
			success: function(data){
                self.set("historyConfig", data);
                if (replaceConfig)
                {
                	Alertify.log.success("Loaded config from the specified commit. Save the job to finalize the change.");
                	self.set("config", data);
                	self.trigger("user_edit");
                }
			},
			error: function(e){
				throw new Error(e.error());
			},
			dataType: "text"
    	})
    },
});
Spawn.AliasModel = Spawn.SelectableModel.extend({
    idAttribute:"name"
    ,url:function(){
        return "/alias/get?id="+this.id;
    }
    ,defaults: {
        name:"(no name)",
        jobs:[]
    },
    save:function(){
        var self=this;
        var postData = {
            name:this.get("name"),
            jobs:this.get("jobs").join(",")
        };
        $.ajax({
            url: "/alias/save",
            type: "POST",
            data: postData,
            success: function(data){
                Alertify.log.success("Alias saved successfully");
                Spawn.aliasCollection.add(data);
                Spawn.router.navigate("#alias/"+data.name,{trigger:true});
            },
            error: function(){
                Alertify.log.error("Error saving alias.");
            },
            dataType: "json"
        });
    },
    delete:function(dontShowAlert){
        var name = this.get("name");
        dontShowAlert = dontShowAlert || false;
        var self = this;
        $.ajax({
            url: "/alias/delete",
            type: "POST",
            data: {name:name},
            success: function(){
                if(!dontShowAlert){
                    Alertify.log.success("Alias deleted successfully");
                    Spawn.router.navigate("#aliases",{trigger:true});
                }
                self.trigger("destroy",self);
            },
            error: function(){
                Alertify.log.error("Error deleting alias.")
            },
            dataType: "json"
        });
    }
});
Spawn.MacroModel = Spawn.SelectableModel.extend({
    idAttribute:"name"
    ,url:function(){
        return "/macro/get?label="+this.id;
    }
    ,defaults: {
        name:"(no name)",
        modified:"",
        owner:"none",
        description:"(no description)",
        macro:"        \n"
    },
    save:function(){
        var self=this;
        var data = this.toJSON();
        data.label=data.name;
        var isNew = this.isNew();
        $.ajax({
            url: "/macro/save",
            type: "POST",
            data: data,
            success: function(data){
                Alertify.log.success("Macro saved successfully.")
                self.set(_.omit(data,"macro"));
                Spawn.macroCollection.add(self);
                Spawn.router.navigate("#macros/"+data.name,{trigger:true});
            },
            error: function(){
                Alertify.log.error("Error saving macro.");
            },
            dataType: "json"
        });
    },
    delete: function(){
        var self = this;
        $.ajax({
            url: "/macro/delete",
            type: "POST",
            data: {name:self.id},
            success: function(){
                Alertify.log.success("Macro deleted successfully.");
                //self.destroy();
                Spawn.macroCollection.remove(self);
                Spawn.router.navigate("#macros",{trigger:true});
            },
            error: function(){
                Alertify.log.error("Error deleting macro.");
            },
            dataType: "json"
        });
    }
});
Spawn.CommandModel = Spawn.SelectableModel.extend({
    idAttribute:"name"
    ,url:function(){
        return "/command/get?command="+this.id;
    }
    ,defaults: {
        name:"(no name)",
        command:[],
        owner:"none",
        reqCPU:1,
        reqIO:1,
        reqMEM:512
    }
    ,save:function(){
        var self=this;
        var data = this.toJSON();
        var isNew = this.isNew();
        data.command = data.command.join(",");
        $.ajax({
            url: "/command/save",
            type: "POST",
            data: data,
            success: function(data){
                Alertify.log.success("Command saved successfully.")
                self.set(data);
                Spawn.commandCollection.add(data);
                Spawn.router.navigate("#commands/"+data.name,{trigger:true});
            },
            error: function(){
                Alertify.log.error("Error saving command.");
            },
            dataType: "json"
        });
    }
    ,delete:function(dontShowAlert){
        var name = this.get("name");
        dontShowAlert = dontShowAlert || false;
        var self = this;
        $.ajax({
            url: "/command/delete",
            type: "POST",
            data: {name:name},
            success: function(){
                if(!dontShowAlert){
                    Alertify.log.success("Command "+name+" deleted successfully.");
                    Spawn.router.navigate("#commands",{trigger:true});
                }
                self.trigger("destroy",self);
            },
            error: function(){
                Alertify.log.error("Error deleting command: "+name);
            },
            dataType: "json"
        });
    }
});
Spawn.CommandCollection = Spawn.SelectableCollection.extend({
	initialize:function(opts){
		opts = opts || {};
		this.collectionName = opts.collectionName || "commandCollection";
		Spawn.SelectableCollection.prototype.initialize.apply(this,[opts]);
		this.search().filter();
	},
    url:"/command/list"
    ,model:Spawn.CommandModel
    ,deleteSelected:function(ids){
        var self = this;
        var count = ids.length;
        _.each(ids,function(id){
            var model = self.get(id);
            if(!_.isUndefined(model)){
                model.delete(true);
            }
        });
        Alertify.log.success("Deleted "+count+" commands");
    }
});
Spawn.AliasCollection = Spawn.SelectableCollection.extend({
	initialize:function(opts){
		opts = opts || {};
		this.collectionName = opts.collectionName || "aliasCollection";
		Spawn.SelectableCollection.prototype.initialize.apply(this,[opts]);
	},
    url:"/alias/list"
    ,model:Spawn.AliasModel
    ,deleteSelected:function(ids){
        var self = this;
        var count = ids.length;
        _.each(ids,function(id){
            var model = self.get(id);
            if(!_.isUndefined(model)){
                model.delete(true);
            }
        });
        Alertify.log.success("Deleted "+count+" commands");
    }
});
Spawn.MacroCollection = Spawn.SelectableCollection.extend({
	initialize:function(data,opts){
		opts = opts || {};
		this.collectionName = opts.collectionName || "macroCollection";
		Spawn.SelectableCollection.prototype.initialize.apply(this,[opts]);
		this.search().filter();
	},
    model:Spawn.MacroModel
    ,deleteSelected:function(ids){
        var self = this;
        _.each(ids,function(id){
            var model = self.get(id);
            if(!_.isUndefined(model)){
                model.delete();
            }
        });
    }
});
Spawn.JobCollection = Spawn.SelectableCollection.extend({
    initialize:function(models,opts){
        opts=opts || {};
        this.url="/job/list";
        this.on("change",this.handleChange);
        this.listenTo(Spawn.server,'job.update',this.handleJobUpdate);
        this.dontAutoUpdate=(_.has(opts,"dontAutoUpdate")?opts.dontAutoUpdate:false);
        this.collectionName=opts.collectionName;
        Spawn.SelectableCollection.prototype.initialize.apply(this,[opts]);
        this.search().filter();
    },
    model: Spawn.JobModel,
    handleJobUpdate:function(jobData){
        if(this.dontAutoUpdate){
            return;
        }
        if(_.isUndefined(this.get(jobData.id))){
            var model = new Spawn.JobModel(jobData);
            this.add(model);
            model.trigger("change",model);
        }
        else{
	        this.get(jobData.id).set(Spawn.JobModel.prototype.parse(jobData));
        }
    },
    kickSelected:function(jobIds){
        var count = jobIds.length;
        var performkick=function() {
            $.ajax({
                url: "/job/start?jobid="+jobIds,
                type: "GET",
                success: function(data){
                    Alertify.log.info(count+" job(s) kicked.",2000)
                },
                error: function(e){
                    Alertify.log.error("Error kicking: "+count+" jobs. <br/> "+e.responseText);
                },
                dataType: "json"
            });
        };
        var confirmkick=function() {
            var jobstring = (count == 1) ? "job" : "jobs";
            Alertify.dialog.confirm(
                "The cluster is quiesced. Are you sure you want to rekick the " + jobstring + "?",
                function (e) {
                    performkick()
                },
                function(e)
                {
                    Alertify.log.info("Job kick canceled.");
                });
        };
        if (Spawn.isQuiesced) {
            confirmkick();
        } else {
            performkick();
        }
    },
    stopSelected:function(jobIds){
        var count = jobIds.length;
		$.ajax({
			url: "/job/stop?jobid="+jobIds,
			type: "GET",
			success: function(data){
                Alertify.log.info(count+" job(s) stopped.",2000)
            },
			error: function(e){
                Alertify.log.error("Error stopping: "+count+" jobs. <br/> "+e.responseText);
			},
			dataType: "json"
		});
    },
    killSelected:function(jobIds){
        var count = jobIds.length;
		$.ajax({
			url: "/job/stop?jobid="+jobIds+"&force=true",
			type: "GET",
			success: function(data){
                Alertify.log.info(count+" job(s) killed.",2000)
			},
			error: function(e){
                Alertify.log.error("Error killing: "+count+" jobs. <br/> "+e.responseText);
			},
			dataType: "json"
		});
    },
    enableBatch:function(jobIds){
        var count = jobIds.length;
        $.ajax({
            url: "/job/enable?jobs="+jobIds+"&enable=1",
            type: "GET",
            success: function(data){
                Alertify.log.info(count+" job(s) enabled.",2000)
            },
            error: function(e){
                Alertify.log.error("Error enabling: "+count+" jobs. <br/> "+e.responseText);
            },
            dataType: "json"
        });
    },
    disableBatch:function(jobIds){
        var count = jobIds.length;
        $.ajax({
            url: "/job/enable?jobs="+jobIds+"&enable=0",
            type: "GET",
            success: function(data){
                Alertify.log.info(count+" job(s) disabled",2000);
            },
            error: function(e){
                Alertify.log.error("Error disabling: "+count+" jobs. <br/> "+e.responseText);
            },
            dataType: "json"
        });
    },
    deleteSelected:function(jobIds){
        var count = jobIds.length;
        var self=this;
        Alertify.dialog.confirm("Are you sure you would like to DELETE "+count+" job?", function (resp) {
            _.each(jobIds,function(jobId){
                var job = self.get(jobId);
                if(!_.isUndefined(job)){
                    job.delete(true);
                }
            });
        });
    }
});
Table={};
Table.HeaderView= Backbone.View.extend({
    tagName: "table",
    className: "table table-striped table-hover table-condensed table-header",
    events: {
        "click a":"handleSort"
    },
    initialize: function(options) {
        _.bindAll(this);
        this.template = _.template($(options.templateId).html());
        if(!_.isUndefined(options.css)){
            this.$el.css(options.css);
        }
        this.downIcon=$("<span>&darr;</span>");
        this.upIcon=$("<span>&uarr;</span>");
        this.transitions={
            "-1":"1",
            "1":"0",
            "0":"-1"
        };
    },
    resort:function(){
        var cookie = $.cookie("sort_"+this.collection.collectionName);
        if(!_.isUndefined(cookie)){
            var column = $(this.$el.find("th").get(cookie.index));
            var orderby = (parseInt(cookie.orderby)*-1)+"";
			if(_.isEqual(orderby,"1")){
				column.append(this.upIcon);
				column.data("orderby","-1");
			}
			else if(_.isEqual(orderby,"-1")){
				column.append(this.downIcon);
				column.data("orderby","1");
			}
            this.sort(column,orderby);
        }
        return this;
    },
    render: function() {
        this.$el=$(this.el);
        var html = this.template();
        this.$el.html(html)
        this.resort();
        return this;
    },
    persistToCookie:function(orderby,index){
        $.cookie("sort_"+this.collection.collectionName,{
            orderby:orderby,
            index:index
        },{
            expires:365
        });
    },
    handleSort: function(event) {
        event.preventDefault();
        event.stopImmediatePropagation();
        var orderby;
        var anchor = $(event.currentTarget);
        var column=anchor.parent();
        if(_.isUndefined(column.data("orderby"))){
            orderby="-1";
        }
        else{
            orderby=this.transitions[column.data("orderby")];
        }
        this.detachIcons();
        if(_.isEqual(orderby,"-1")){
            column.append(this.upIcon);
        }
        else if(_.isEqual(orderby,"1")){
            column.append(this.downIcon);
        }
		column.data("orderby",orderby);
		this.persistToCookie(orderby,column.index());
        this.sort(column,orderby);
    },
    detachIcons:function(){
        this.downIcon.detach();
        this.upIcon.detach();
    },
    sort:function(column,orderby){
        column=$(column);
        var index = (column.is("th")?column.index():column.parent().index());
        var table = this.$el.find("div.table-body-full-height table.table tbody");
        var rows = table.children("tr.row");
        var detachedList = [];
        var fieldName = column.data("name");//column.parent().data("name");
        this.orderCoeff = parseInt(orderby);
        var self=this;
        this.collection.comparator = function(rowA,rowB){
            var valA = rowA.get(fieldName);
            var valB = rowB.get(fieldName);
            if( (!_.isUndefined(valA) && _.isUndefined(valB)) || (valA<valB)){
                return -1*self.orderCoeff;
            }
            else if( (_.isUndefined(valA) && !_.isUndefined(valB)) || (valA>valB)){
                return 1*self.orderCoeff;
            }
            else{
                return 0;
            }
        };
        this.collection.sort();
    }
});
Table.RowView = Backbone.View.extend({
    tagName: "tr",
    className: "selectable",
    events: {
    },
    initialize: function(options) {
        _.bindAll(this);
        this.template=options.template;
        this.listenTo(this.model,"destroy",this.handleDestroy);
        this.listenTo(this.model,"change",this.render);
        this.listenTo(this.model,"app:hide",this.hide);
        this.listenTo(this.model,"app:show",this.show);
        this.listenTo(this.model,"app:select",this.render);
        this.listenTo(this.model,"app:unselect",this.render);
    },
    render: function() {
        this.$el=$(this.el);
        var html = this.template({
            row:this.model.toJSON()
            ,isSelected:this.model.isSelected
        });
        this.$el.attr("data-id",this.model.id);
        if(this.model.isSelected){
            this.$el.addClass("selected");
        }
        else{
            this.$el.removeClass("selected");
        }
        if(this.model.isHidden){
            this.hide();
        }
        else{
            this.show();
        }
        this.$el.html(html);
        this.$el.find(".init-tooltip").tooltip();
        return this;
    },
    rowClickHandler: function(event){
    },
    hide:function(){
        this.model.isHidden=true;
        this.$el.hide();
    },
    show:function(){
        this.model.isHidden=false;
        this.$el.show();
    },
    select:function(){
        if(!this.model.isHidden){
            this.model.isSelected=true;
            this.$el.addClass("selected");
        }
    },
    unselect:function(){
        if(!this.model.isHidden){
            this.model.isSelected=false;
            this.$el.removeClass("selected");
        }
    },
    handleDestroy:function(model){
        this.stopListening();
        this.$el.remove();
    }
});
Table.BodyView = Backbone.View.extend({
    tagName: "table",
    className: "table table-condensed table-body",
    events: {
        "click tr":"rowClickHandler"
    },
    initialize: function(options) {
        _.bindAll(this);
        this.collection.once("reset",this.render);
        this.listenTo(this.collection,"sort",this.handleSort);
        this.listenTo(this.collection,"add",this.handleAdd);
        this.listenTo(this.collection,"remove",this.handleRemove);
        this.listenTo(this.collection,"change",this.handleChange);
        this.template=_.template($(options.templateId).html());
        this.hasRendered=false;
        this.selected=options.selected || {};
        this.Views={};
        if(!_.isUndefined(options.css)){
            this.$el.css(options.css);
        }
    },
    render: function() {
        this.$el=$(this.el);
        this.$el.empty();
        var rowTemplate = this.template;
        var self=this;
        this.collection.each(function(model){
            var row = new Table.RowView({
                model: model,
                template:rowTemplate
            }).render();
            self.Views[model.id]=row;
            self.$el.append(row.$el);
        });
        this.hasRendered=true;
        return this;
    },
    rowClickHandler: function(event){
        event.preventDefault();
    },
    handleSort:function(collection){
		var self=this;
		collection.each(function(model){
			try{
				var rowView = self.Views[model.id];
				self.$el.prepend(rowView.$el);

			}
			catch(err){
				console.log("error getting: "+model.id);
			}
		});
    },
    handleAdd:function(model){
        var row = new Table.RowView({
            model: model,
            template:this.template
        }).render();
        this.Views[model.id]=row;
        this.$el.append(row.$el);
        return this;
    },
    handleChange:function(job){
    },
    handleRemove:function(model){
        if(_.has(this.Views,model.id)){
            this.Views[model.id].$el.remove();
        }
        return this;
    }
});
Spawn.JobsTable = Backbone.View.extend({
    tagName:"div",
    className:"span12",
    events:{
        "click tr":"rowClickHandler",
        "click a":"anchorClickHandler",
        "click a#compactTable":"handleCompactTableClick",
        "click a#comfortableTable":"handleComfortableTableClick"
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
        if(!_.isUndefined(options.css)){
            this.$el.css(options.css);
        }
        this.batchActionClass=(_.has(options,"batchActionClass")?options.batchActionClass:Spawn.JobBatchActions);
        this.dontShowNewButton = options.dontShowNewButton || false;
        this.titleView = options.titleView;
    },
    isCompact:function(){
        return !_.isUndefined($.cookie("compact")) && _.isEqual(parseInt($.cookie("compact")),1);
    },
    render:function(){
        this.$el.html("");
        this.views.batchActions = new this.batchActionClass({
            collection:this.collection
            ,dontShowNewButton:this.dontShowNewButton
            ,titleView:this.titleView
        }).render();
        this.views.batchActions.$el.css({
            "z-index":"100"
        });
        this.views.batchActions.$el.appendTo(this.$el);
        this.views.header = new Table.HeaderView({
            templateId: "#job"+(this.isCompact()?"Compact":"")+"TableHeaderTemplate",//jobHeaderTemplate,//
            collection: this.collection
        }).render();
        this.views.header.$el.css({
            "z-index":"100"
        });
        this.views.body = new Table.BodyView({
            collection: this.collection,
            templateId: "#job"+(this.isCompact()?"Compact":"")+"TableRowTemplate",//jobHeaderTemplate,//
            selected:this.selected
        }).render();
        this.views.bodyContainer=$("<div class='table-body-full-height'></div>").append(this.views.body.$el);
        this.views.header.$el.appendTo(this.$el);
        this.views.bodyContainer.appendTo(this.$el);
        this.$el.show();
        return this;
    },
    rowClickHandler:function(event){
        var target = $(event.target);
        if(target.is("tr.selectable") || target.is("tr.selectable td") || target.is("tr.selectable td input")){
            var id = $(event.currentTarget).data("id");
            var job = this.collection.get(id);
            var name = job.get("id");
            if(job.isSelected){
                job.unselect();
            }
            else{
                job.select();
            }
        }
        return this;
    },
    handleComfortableTableClick:function(event){
        var cookie = $.cookie("comfortable");
        var compact=0,comfortable;
        if(_.isUndefined(cookie)){
            comfortable=1;
        }
        else{
            comfortable=(parseInt(cookie)+1)%2;
        }
		$.cookie("compact",compact,{expires:365});
		$.cookie("comfortable",comfortable,{expires:365});
        this.render();
    },
    handleCompactTableClick:function(event){
        var cookie = $.cookie("compact");
        var compact,comfortable;
		comfortable=0;
        if(_.isUndefined(cookie)){
            compact=1;
        }
        else{
            compact=(parseInt(cookie)+1)%2;
        }
		$.cookie("compact",compact,{ expires: 365 });
		$.cookie("comfortable",comfortable,{ expires: 365 });
        this.render();
    },
    anchorClickHandler:function(event){
        var anchor = $(event.currentTarget);
        if(anchor.is("a[data-bypass]")){
            window.open(anchor.attr("href"),'_blank');
        }
        else{
            Spawn.router.navigate(anchor.attr("href"),{trigger:true});
        }
    },
    windowResizeHandler:function(event){
        this.resize($(window).height());
    },
    resize:function(height){
    },
    show:function(){
        this.$el.show();
        return this;
    },
    hide:function(){
        this.$el.hide();
        return this;
    }
});
Spawn.GangliaGraphView = Backbone.View.extend({
	initialize:function(opts){

	},
	render:function(){
		var host = this.model;
		this.$el.html("ganglia graph for "+host.get("uuid"));
		return this;
	},
    close:function(){
        this.$el.remove();
        return this;
    },
    show:function(){
        this.$el.show();
    },
});
Spawn.SelectableTableView = Backbone.View.extend({
    tagName:"div",
    className:"span12",
    events:{
        "click tr":"rowClickHandler",
        "click a":"anchorClickHandler"
    },
    initialize:function(options){
        //_.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
        if(!_.isUndefined(options.css)){
            this.$el.css(options.css);
        }
    },
    render:function(){
        if(!this.hasRendered){
            this.views.batchActions = new this.batchActionClass({
            collection:this.collection
            }).render();
            this.views.batchActions.$el.css({
                "z-index":"100"
            });
            this.views.batchActions.$el.appendTo(this.$el);
            this.views.header = new Table.HeaderView({
                templateId: this.headerTemplate,
                collection:this.collection
            }).render();
            this.views.header.$el.css({
                "z-index":"100"
            });
            this.views.body = new Table.BodyView({
                collection: this.collection,
                templateId: this.tableRowTemplate,
                selected:this.selected
            }).render();
            this.views.bodyContainer=$("<div class='table-body-full-height'></div>").append(this.views.body.$el);
            this.views.header.$el.appendTo(this.$el);
            this.views.bodyContainer.appendTo(this.$el);
        }
        this.$el.show();
        this.hasRendered=true;
        return this;
    },
    rowClickHandler:function(event){
        var target = $(event.target);
        if(target.is("tr.selectable") || target.is("tr.selectable td") || target.is("tr.selectable td input")){
            var id = $(event.currentTarget).data("id");
            var model = this.collection.get(id);
            var name = model.id;
            if(model.isSelected){
                model.unselect();
            }
            else{
                model.select();
            }
        }
        return this;
    },
    anchorClickHandler:function(event){
        var anchor = $(event.currentTarget);
        if(anchor.is("a[data-bypass]")){
            window.open(anchor.attr("href"),'_blank');
        }
        else{
            Spawn.router.navigate(anchor.attr("href"),{trigger:true});
        }
    },
    windowResizeHandler:function(event){
        this.resize($(window).height());
    },
    resize:function(height){
    },
    show:function(){
        this.$el.show();
        return this;
    },
    hide:function(){
        this.$el.hide();
        return this;
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        return this;
    }
});
Spawn.HostsTable = Backbone.View.extend({
    tagName:"div",
    className:"span12",
    events:{
        "click tr":"rowClickHandler",
        "click a":"anchorClickHandler"
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
        if(!_.isUndefined(options.css)){
            this.$el.css(options.css);
        }
    },
    render:function(){
        if(!this.hasRendered){
            this.views.batchActions = new Spawn.HostBatchActions({
                collection:this.collection
            }).render();
            this.views.batchActions.$el.css({
                "z-index":"100"
            });
            this.views.batchActions.$el.appendTo(this.$el);
            this.views.header = new Table.HeaderView({
                templateId: "#hostTableHeaderTemplate",
                collection:this.collection
            }).render();
            this.views.header.$el.css({
                "z-index":"100"
            });
            this.views.body = new Table.BodyView({
                collection: this.collection,
                templateId: "#hostTableRowTemplate",
                selected:this.selected
            }).render();
            this.views.bodyContainer=$("<div class='table-body-full-height'></div>").append(this.views.body.$el);
            this.views.header.$el.appendTo(this.$el);
            this.views.bodyContainer.appendTo(this.$el);
        }
        this.$el.show();
        this.hasRendered=true;
        return this;
    },
    rowClickHandler:function(event){
        var target = $(event.target);
        if(target.is("tr.selectable") || target.is("tr.selectable td") || target.is("tr.selectable td input")){
            var id = $(event.currentTarget).data("id");
            var job = this.collection.get(id);
            var name = job.get("id");
            if(job.isSelected){
                job.unselect();
            }
            else{
                job.select();
            }
        }
        return this;
    },
    anchorClickHandler:function(event){
        var anchor = $(event.currentTarget);
        if(anchor.is("a[data-bypass]")){
            window.open(anchor.attr("href"),'_blank');
        }
        else{
            Spawn.router.navigate(anchor.attr("href"),{trigger:true});
        }
    },
    windowResizeHandler:function(event){
        this.resize($(window).height());
    },
    resize:function(height){
    },
    show:function(){
        this.$el.show();
        return this;
    },
    hide:function(){
        this.$el.hide();
        return this;
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        return this;
    }
});
Spawn.SideFilterView = Backbone.View.extend({
    className:"span1 sidebar",
    events:{
        "click a":"handleClick"
    },
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
        });
        this.selected={};
        this.views={};
        this.criteria={};
        this.listenTo(this.collection,"change",this.handleCollectionChange);
    },
    render:function(){
        this.$el=$(this.el);
        return this;
    },
    handleClick:function(event){
        var anchor = $(event.currentTarget);
        var li = anchor.parent();
        var ul = li.parent();
        var key = ul.data("key");
        if(_.isEqual(key,"*")){
            this.collection.search();
        }
        else{
            if(li.hasClass("active")){
                this.criteria=_.omit(this.criteria,key);
            }
            else{
                var value = anchor.data(key);
                this.criteria[key]=value;
            }
            this.collection.filter(this.criteria);
        }
        if(anchor.data("bypass")){
            Spawn.router.navigate(anchor.attr("href"),{trigger:true});
        }
    },
    clearFilter:function(){
        this.collection.clear();
    }
});
Spawn.MacroFilterView = Spawn.SideFilterView.extend({
    initialize:function(){
        _.bindAll(this);
        this.views={};
        this.$el.css({
            "margin-left":"0px"
        });
        this.selected={};
        this.views={};
        this.criteria={};
        this.collection=Spawn.macroCollection;
        if(!_.isUndefined(this.collection.collectionName) && !_.isUndefined($.cookie(this.collectionName))){
			var cookie = $.cookie(this.collection.collectionName);
			this.criteria = cookie.criteria || {};
        }
        else{
			this.criteria= {};
        }
        this.listenTo(this.collection,"change",this.handleCollectionChange);
    },
    render:function(){
        //Sidebar: All
        this.views.sidebarAll = new Spawn.SidebarView({
            template: _.template($("#macroSidebarAllTemplate").html()),
            key:"*",
            collection:this.collection
        }).render();
        this.views.sidebarUp = new Spawn.SidebarView({
            template: _.template($("#ownerFilterTemplate").html()),
            key:"owner",
            collection:this.collection
        }).render();
        this.$el.append(this.views.sidebarAll.$el);
        this.$el.append(this.views.sidebarUp.$el);
        return this;
    }
});
Spawn.HostSideFilter = Backbone.View.extend({
    className:"span1 hosts-sidebar",
    events:{
        "click a":"handleClick"
    },
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
        });
        this.selected={};
        this.views={};
        this.criteria={};
        if(!_.isUndefined(this.collection.collectionName) && !_.isUndefined($.cookie(this.collectionName))){
			var cookie = $.cookie(this.collection.collectionName);
			this.criteria = cookie.criteria || {};
        }
        else{
			this.criteria= {};
        }
        this.listenTo(this.collection,"change",this.handleCollectionChange);
    },
    render:function(){
        this.$el=$(this.el);
        this.views.sidebarAll = new Spawn.SidebarView({
            template: _.template($("#hostSidebarAllTemplate").html()),
            key:"*",
            collection:this.collection
        }).render();
        this.views.sidebarUp = new Spawn.SidebarView({
            template: _.template($("#hostSidebarUpTemplate").html()),
            key:"up",
            collection:this.collection
        }).render();
        this.$el.append(this.views.sidebarAll.$el);
        this.$el.append(this.views.sidebarUp.$el);
        return this;
    },
    handleClick:function(event){
        var anchor = $(event.currentTarget);
        var li = anchor.parent();
        var ul = li.parent();
        var key = ul.data("key");
        if(_.isEqual(key,"*")){
            this.collection.search();
        }
        else{
            if(li.hasClass("active")){
                this.criteria=_.omit(this.criteria,key);
            }
            else{
                var value = anchor.data(key);
                this.criteria[key]=value;
            }
            this.collection.filter(this.criteria);
        }
    },
    clearFilter:function(){
        this.collection.clear();
    }
});
Spawn.JobsSideFilter = Backbone.View.extend({
    className:"span1 jobs-sidebar",
    events:{
        "click a":"handleClick"
    },
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
        });
        this.selected={};
        this.views={};
        if(!_.isUndefined(this.collection.collectionName) && !_.isUndefined($.cookie(this.collectionName))){
			var cookie = $.cookie(this.collection.collectionName);
			this.criteria = cookie.criteria || {};
			this.filter();
        }
        else{
			this.criteria= {};
        }
        this.listenTo(this.collection,"change",this.handleCollectionChange);
    },
    render:function(){
        this.$el=$(this.el);
        //Sidebar: All
        this.views.sidebarAll = new Spawn.SidebarView({
            template: _.template($("#jobSidebarAllTemplate").html()),
            key:"*",
            collection:this.collection
        }).render();
        //Sidebar: Job State
        this.views.sidebarState = new Spawn.SidebarView({
            template: _.template($("#jobSidebarStateTemplate").html()),
            key:"state",
            collection:this.collection
        }).render();
        this.views.sidebarCreator = new Spawn.SidebarView({
            template: _.template($("#jobSidebarCreatorTemplate").html()),
            key:"creator",
            collection:this.collection
        }).render();
        //Sidebar: Job Submit Time
        this.$el.append(this.views.sidebarAll.$el);
        this.$el.append(this.views.sidebarState.$el);
        this.$el.append(this.views.sidebarCreator.$el);
        this.collection.filter();
        return this;
    },
    handleClick:function(event){
        var anchor = $(event.currentTarget);
        var li = anchor.parent();
        var ul = li.parent();
        var key = ul.data("key");
        if(_.isEqual(key,"*")){
            this.collection.search();
            this.criteria={};
        }
        else{
            if(li.hasClass("active")){
                this.criteria=_.omit(this.criteria,key);
            }
            else{
                var value = anchor.data(key);
                this.criteria[key]=value;
            }
        }
        this.filter();
    },
    clearFilter:function(){
        this.collection.clear();
    },
    handleCollectionChange:function(model){
    },
    filter:function(key,value){
		this.collection.filter(this.criteria);
    }
});
Spawn.ActionButton = Backbone.View.extend({
    tagName:"button",
    className:"btn btn-mini",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        if(!_.isUndefined(options.css)){
            this.$el.css(_.extend({
                "display":"inline-block",
                "vertical-align":"top",
                "display":"-moz-inline-stack"
            },options.css));
        }
    },
    render:function(){
        var text = this.text;
        this.$el.html(text);
        return this;
    }
});
Spawn.BatchButton = Backbone.View.extend({
    tagName:"button",
    className:"btn btn-mini batch-action",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.listenTo(this.collection,"remove",this.handleJobRemove);
        this.listenTo(this.collection,"app:select",this.handleJobSelect);
        this.listenTo(this.collection,"app:unselect",this.handleJobUnselect);
        this.listenTo(this.collection,"app:show",this.handleShow);
        this.listenTo(this.collection,"app:hide",this.handleHide);
        if(!_.isUndefined(options.css)){
            this.$el.css(_.extend({
                "display":"inline-block",
                "vertical-align":"top",
                "display":"-moz-inline-stack"
            },options.css));
        }
        this.selectedIds= _.clone(options.selectedIds) || {};
        this.selected=(!_.isEmpty(this.selectedIds)?_.keys(this.selectedIds).length:0);
    },
    render:function(){
        var text = this.text;
        if(this.selected>0){
            this.$el.css("display","inline");
        }
        else{
            this.$el.hide();
        }
        this.$el.html(text);
        return this;
    },
    handleJobSelect:function(model){
        if(!_.has(this.selectedIds,model.id)){
            this.selected=this.selected+1;
            this.render();
            this.selectedIds[model.id]=1;
        }
    },
    handleJobRemove:function(model){
        if(_.has(this.selectedIds,model.id)){
            this.selected=this.selected-1;
            this.render();
            delete this.selectedIds[model.id];
        }
    },
    handleJobUnselect:function(model){
        if(_.has(this.selectedIds,model.id)){
            this.selected=this.selected-1;
            this.render();
            delete this.selectedIds[model.id];
        }
    },
    handleShow:function(model){
        if(_.has(this.selectedIds,model.id) && _.isEqual(this.selectedIds[model.id],0)){
            this.selectedIds[model.id]=1;
            this.selected++;
            this.render();
        }
    },
    handleHide:function(model){
        if(_.has(this.selectedIds,model.id) && _.isEqual(this.selectedIds[model.id],1)){
            this.selectedIds[model.id]=0;
            this.selected--;
            this.render();
        }
    },
    getVisibleIds:function(){
        var visibleIds=[];
        var self=this;
        _.each(_.keys(this.selectedIds),function(key){
            if(_.isEqual(self.selectedIds[key],1)){
                visibleIds.push(key);
            }
        });
        return visibleIds;
    }
});
Spawn.CreateButton = Backbone.View.extend({
    tagName:"button",
    className:"btn btn-mini batch-action",
    events:{
        "click":"handleClick"
    },
    initialize:function(options){
        this.$el.css({
            "margin-left":"0px"
            ,"width":"100px"
        });
        this.title = (options.title?options.title:"Create");
        this.createUrl = (options.createUrl?options.createUrl:"#new");
    },
    render:function(){
        this.$el.html(this.title);
        return this;
    },
    handleClick:function(){
        Spawn.router.navigate(this.createUrl,{trigger:true});
        return true;
    }
});
Spawn.NewJobButton = Backbone.View.extend({
    tagName:"button",
    className:"btn btn-mini batch-action",
    events:{
        "click":"handleClick"
    },
    initialize:function(options){
        this.$el.css({
            "margin-left":"0px"
            ,"width":"100px"
        });
    },
    render:function(){
        this.$el.html("Create");
        return this;
    },
    handleClick:function(){
        Spawn.router.navigate("#jobs/new/conf",{trigger:true});
        return true;
    }
});
Spawn.KickBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"kickSelectedJobs"
    },
    kickSelectedJobs:function(){
        this.collection.kickSelected(this.getVisibleIds());
    },
    text:"Kick"
});
Spawn.RebalanceBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"rebalanceSelectedHosts"
    },
    rebalanceSelectedHosts:function(){
        this.collection.rebalanceBatch(this.getVisibleIds());
    },
    text:"Rebalance"
});
Spawn.FailBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"failSelectedHosts"
    },
    failSelectedHosts:function(){
        this.collection.failBatch(this.getVisibleIds());
    },
    text:"Fail"
});
Spawn.DropBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"dropSelected"
    },
    dropSelected:function(){
        this.collection.dropBatch(this.getVisibleIds());
    },
    text:"Drop"
});
Spawn.DisableBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"disableSelected"
    },
    disableSelected:function(){
        this.collection.disableBatch(this.getVisibleIds());
    },
    text:"Disable"
});
Spawn.StopBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"stopSelectedJobs"
    },
    stopSelectedJobs:function(){
        this.collection.stopSelected(this.getVisibleIds());
    },
    text:"Stop"
});
Spawn.KillBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"killSelectedJobs"
    },
    killSelectedJobs:function(){
        this.collection.killSelected(this.getVisibleIds());
    },
    text:"Kill"
});
Spawn.DeleteBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"deleteSelected"
    },
    deleteSelected:function(){
        this.collection.deleteSelected(this.getVisibleIds());
    },
    text:"Delete"
});
Spawn.PartialCheckbox = Backbone.View.extend({
    tagName:"span",
    className:"checkbox",
    events:{
        "click":"handleClick"
    },
    render:function(){
        this.$el.html("<span class='checkbox'></span>");
        return this;
    },
    handleClick:function(){
        this.$el.addClass("checked");
    }
});
Spawn.EnableBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"enableSelectedJobs"
    },
    enableSelectedJobs:function(){
        this.collection.enableBatch(this.getVisibleIds());
    },
    text:"Enable"
});
Spawn.EnableBatchHostsButton = Spawn.BatchButton.extend({
    events:{
        "click":"enableHosts"
    },
    enableHosts:function(){
        this.collection.toggleBatchHosts(this.getVisibleIds(), false);
    },
    text:"Enable"
});
Spawn.DisableBatchHostsButton = Spawn.BatchButton.extend({
    events:{
        "click":"disableHosts"
    },
    disableHosts:function(){
        this.collection.toggleBatchHosts(this.getVisibleIds(), true);
    },
    text:"Disable"
});
Spawn.SelectBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"handleClick"
    },
    render:function(){
        var html = "";
        if(_.isEqual(this.selected,this.collection.length) && this.collection.length>0){
            html+="<span class='checkbox checked-all'></span>";
        }
        else if(this.selected>0){
            html+="<span class='checkbox checked-partial'></span>";
        }
        else{
            html+="<span class='checkbox'></span>";
        }
        html+=" ("+this.selected+")";
        this.$el.html(html);
        return this;
    },
    handleClick:function(){
        var visibleIds = this.getVisibleIds();
        var self = this;
        if(this.selected>0){
            _.each(visibleIds,function(jobId){
                self.collection.get(jobId).unselect();
            });
        }
        else{
            self.collection.selectAll();
        }
    }
});
Spawn.TypeaheadBox = Backbone.View.extend({
    tagName:"div",
    className: "search-query span3 clearable",
    events: {
        "change":"handleChange",
        "blur":"handleChange",
        "keypress":"handleKeypress",
        "click span.icon_clear":"closeIconClickHandler"
    },
    initialize: function(opts) {
        var self = this;
        if(_.has(opts,"placeholder")){
            this.placeholder=opts.placeholder;
        }
        this.keys=opts.keys;
        this.conf = {
            autoSelect: false,
            source: function(){
                var results={};
                self.collection.forEach(function(job){
                    var sub = _.pick(job.toJSON(),opts.keys);
                    var vals = _.values(sub);
                    _.each(vals,function(val){
                        results[val.toString()]=0;
                    });
                });
                return _.keys(results);
            },
            updater:function(value){
                var text = "";
                if(_.isUndefined(value) || _.isNull(value)){
                    text=self.views.input.val();
                }
                else{
                    text=value;
                }
                var query = (_.isUndefined(text) || _.isNull(text) ? self.getValue():text);
                if(_.isEmpty(query)){
                    self.handleClear();
                }
                else{
                    self.handleSearch({
                        query:query,
                        keys:opts.keys
                    });
                }
                return query;
            }
        };
    },
    render: function() {
        this.$el.html("");
        this.views={
            input: $("<input class='data_field' type='text'></input>"),
            closeIcon: $("<span class='icon_clear'>X</span>")
        };
        if(!_.isUndefined(this.placeholder)){
            this.views.input.attr("placeholder",this.placeholder);
        }
        var searchQuery = this.collection.searchQuery;
        if(!_.isEmpty(searchQuery)){
            this.views.input.val(searchQuery.query);
        }
        this.views.input.typeahead(this.conf);
        this.$el.append(this.views.closeIcon);
        this.$el.append(this.views.input);
        this.checkCloseIcon();
        return this;
    },
    handleChange: function(event){
        var value = this.getValue();
        var isEmpty = _.isEmpty(value);
        if(_.isEmpty(this.getValue())){
            this.collection.clear();
        }
        else{
            this.handleSearch({
                query:this.getValue(),
                keys:this.keys
            });
        }
        this.checkCloseIcon();
    },
    handleClear:function(){
        this.collection.clear();
        this.checkCloseIcon();
    },
    handleSearch:function(searchQuery){
        this.searchQuery=searchQuery;
        this.collection.search(searchQuery);
        this.collection.clearFilter();
        this.checkCloseIcon();
    },
    handleKeypress:function(event){
        var code = (event.keyCode ? event.keyCode : event.which);
        if (_.isEqual(code,13)) { //Enter keycode
            event.preventDefault();
            event.stopImmediatePropagation();
            if(_.isEmpty(this.getValue())){
                this.collection.clear();
            }
            else{
                this.handleSearch({
                    query:this.getValue(),
                    keys:this.keys
                });
            }
            this.checkCloseIcon();
        }
    },
    getValue: function(){
        return this.views.input.val();
    },
    checkCloseIcon:function(){
		var io = this.views.input.val().length ? 1 : 0 ;
		this.views.closeIcon.stop().fadeTo(300,io);
    },
    closeIconClickHandler:function(){
		 this.views.closeIcon.delay(300).fadeTo(300,0);
		 this.views.input.val("");
		 this.handleClear();
    }
});
Spawn.JobTypeaheadBox = Spawn.TypeaheadBox.extend({
    initialize: function(opts) {
        var self = this;
        this.$el.find("input").attr("placeholder","Search jobs...");
        this.keys=["id","creator","description","alerts"];
        this.conf = {
            autoSelect: false,
            source: function(){
                var results={};
                self.collection.forEach(function(job){
                    var sub = _.pick(job.toJSON(),["id","creator","description","alerts"]);
                    var vals = _.values(sub);
                    _.each(vals,function(val){
                        if(_.isArray(val)){
                            var emails = _.pluck(val,"email");
                            _.each(emails,function(email){
                                results[email]=0;
                            });
                        }
                        else{
                            results[val.toString()]=0;
                        }
                    });
                });
                return _.keys(results);
            },
            updater:function(value){
                var text = "";
                if(_.isUndefined(value) || _.isNull(value)){
                    text = self.$el.val();
                }
                else{
                    text=value;
                }
                var query = (_.isUndefined(text) || _.isNull(text) ? self.getValue():text);
                if(_.isEmpty(query)){
                    self.handleClear();
                }
                else{
                    self.handleSearch({
                        query:query,
                        keys:opts.keys
                    });
                }
                return query;
            }
        };
    }
});
Spawn.KickTaskBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"kickSelectedTasks"
    },
    kickSelectedTasks:function(){
        var self=this,count=0;
        _.each(this.getVisibleIds(),function(taskNum){
            self.collection.get(taskNum).kick();
            count++;
        });
        Alertify.log.info(count+" task(s) kicked.",2000);
    },
    text:"Kick"
});
Spawn.StopTaskBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"stopSelectedTasks"
    },
    stopSelectedTasks:function(){
        var self=this,count=0;
        _.each(this.getVisibleIds(),function(taskNum){
            self.collection.get(taskNum).stop();
            count++;
        });
        Alertify.log.info(count+" task(s) stopped.",2000);
    },
    text:"Stop"
});
Spawn.KillTaskBatchButton = Spawn.BatchButton.extend({
    events:{
        "click":"killSelectedTasks"
    },
    killSelectedTasks:function(){
        var self=this;
        var count=0;
        _.each(this.getVisibleIds(),function(taskNum){
            self.collection.get(taskNum).kill();
            count++;
        });
        Alertify.log.info(count+" task(s) killed.",2000);
    },
    text:"Kill"
});
Spawn.BreadcrumbView = Backbone.View.extend({
    tagName:"ul",
    className:"breadcrumb",
    events:{
        "click a":"handleAnchorClick"
    },
    initialize:function(options){
        _.bindAll(this);
        this.views={};
        this.$el.css({
            "display":"inline",
            "background-color":"transparent",
        });
        if(_.has(options,"template")){
            this.template=options.template;
        }
    },
    render:function(){
        var html = this.template(this.model);
        this.$el.html(html);
        return this;
    },
    handleAnchorClick:function(event){
    }
});
Spawn.SingleTaskActions = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "background":"#EEE",
            "padding-top":"2px",
            "margin-left":"0px"
        });
        if(_.has(options,"taskNum")){
            this.taskNum=options.taskNum;
        }
        this.hasRendered=false;
        this.listenTo(this.collection,"reset",this.render);
    },
    render:function(){
        if(this.hasRendered){
            this.views.breadcrumb.$el.empty();
            this.$el.empty();
        }
        this.views={
            breadcrumb: new Spawn.BreadcrumbView({
                template:_.template($("#taskBreadcrumbsTemplate").html()),
                model:{
                    jobId:this.model.id,
                    node:this.taskNum
                }
            }).render(),
            logControls:new Spawn.TaskLogControls({
            }).render(),
            taskButtons: new Spawn.TaskActionButtons({
                model:this.model,
                collection:this.collection,
                taskNum:this.taskNum
            }).render()
        };
        this.views.breadcrumb.$el.appendTo(this.$el);
        this.views.taskButtons.$el.appendTo(this.$el);
        this.views.logControls.$el.appendTo(this.$el);
        this.hasRendered=true;
        return this;
    }
});
Spawn.TaskBatchActions = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "background":"#EEE",
            "padding-top":"2px",
            "margin-left":"0px"
        });
    },
    render:function(){
        this.views={
            breadcrumb: new Spawn.BreadcrumbView({
                template:_.template($("#taskBreadcrumbsTemplate").html()),
                model:{
                    jobId:this.model.id,
                    node:""
                }
            }).render(),
            kickTaskButton:new Spawn.KickTaskBatchButton({
                collection:this.collection,
                css:{
                    "color":"green",
                    "margin-top":"2px"
                }
            }).render(),
            stopTaskButton:new Spawn.StopTaskBatchButton({
                collection:this.collection,
                css:{
                    "margin-left":"10px",
                    "color":"red",
                    "margin-top":"2px"
                }
            }).render(),
            killTaskButton:new Spawn.KillTaskBatchButton({
                collection:this.collection,
                css:{
                    "margin-left":"10px",
                    "color":"red",
                    "margin-top":"2px"

                }
            }).render(),
            selectTaskButton:new Spawn.SelectBatchButton({
                collection:this.collection,
                css:{
                    "text-align":"left",
                    "width":"64px",
                    "margin-left":"20px",
                    "margin-top":"2px"

                }
            }).render(),
            searchbox: new Spawn.TypeaheadBox({
                collection:this.collection,
                className:"search-query pull-right clearable",
                //keys:["node","hostUrl","hostUuid"],
                keys:["host","hostUuid"],
                placeholder:"Search Tasks.."
            }).render()
        };
        this.views.breadcrumb.$el.appendTo(this.$el);
        this.views.selectTaskButton.$el.appendTo(this.$el);
        this.views.kickTaskButton.$el.appendTo(this.$el);
        this.views.stopTaskButton.$el.appendTo(this.$el);
        this.views.killTaskButton.$el.appendTo(this.$el);
        this.views.searchbox.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.HostBatchActions = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "background":"#EEE",
            "padding-top":"4px"
        });
    },
    render:function(){
        this.views={
            selectButton:new Spawn.SelectBatchButton({
                collection:this.collection,
                css:{
                    "width":"64px",
                    "margin-left":"10px"
                }
            }).render(),
            rebalanceButton: new Spawn.RebalanceBatchButton({
                collection:this.collection,
                css:{
                    "width":"68px",
                    "margin-left":"20px",
                    "color":"blue"
                }
            }).render(),
            failButton: new Spawn.FailBatchButton({
                collection:this.collection,
                css:{
                    "width":"58px",
                    "margin-left":"20px",
                    "color":"red"
                }
            }).render(),
            dropButton: new Spawn.DropBatchButton({
                collection:this.collection,
                css:{
                    "width":"58px",
                    "margin-left":"10px",
                    "color":"red"
                }
            }).render(),
            enableButton: new Spawn.EnableBatchHostsButton({
                collection:this.collection,
                css:{
                    "width":"58px",
                    "margin-left":"10px",
                    "color":"black"
                }
            }).render(),
            disableButton: new Spawn.DisableBatchHostsButton({
                collection:this.collection,
                css:{
                    "width":"58px",
                    "margin-left":"10px",
                    "color":"black"
                }
            }).render(),
            searchbox: new Spawn.TypeaheadBox({
                collection:this.collection,
                className:"search-query pull-right",
                keys:["uuid","host"],
                placeholder:"Search Hosts.."
            }).render()
        };
        this.views.selectButton.$el.appendTo(this.$el);
        this.views.rebalanceButton.$el.appendTo(this.$el);
        this.views.failButton.$el.appendTo(this.$el);
        this.views.dropButton.$el.appendTo(this.$el);
        this.views.enableButton.$el.appendTo(this.$el);
        this.views.disableButton.$el.appendTo(this.$el);
        this.views.searchbox.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.DisplayDensityButton = Backbone.View.extend({
	className:"btn-mini btn-group pull-right",
	initialize:function(opts){
		this.$el.css({
			"vertical-align":"top",
			"margin-left":"5px",
			"color":"black",
			"display":"inline"
		});
	},
	template:_.template($("#displayDensityButtonTemplate").html()),
	render:function(){
		this.$el.html(this.template({
			compact: $.cookie("compact") || 0,
			comfortable: $.cookie("comfortable") || 0
		}));
		return this;
	}
});
Spawn.JobBatchActions = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selectedIds={};
        this.views={};
        this.$el.css({
            "background":"#EEE",
            "padding-top":"4px"
        });
        this.dontShowNewButton = options.dontShowNewButton || false;
        this.titleView = options.titleView;
    },
    render:function(){
        this.$el.html("");
        this.selectedIds=this.collection.getSelectedIdMap();
        this.views={
            kickJobButton:new Spawn.KickBatchButton({
                collection:this.collection,
                css:{
                    "color":"green"
                }
                ,selectedIds: this.selectedIds
            }).render(),
            stopJobButton:new Spawn.StopBatchButton({
                collection:this.collection,
                css:{
                    "margin-left":"10px",
                    "color":"red"
                }
                ,selectedIds: this.selectedIds
            }).render(),
            killJobButton:new Spawn.KillBatchButton({
                collection:this.collection,
                css:{
                    "margin-left":"10px",
                    "color":"red"
                }
                ,selectedIds: this.selectedIds
            }).render(),
            enableJobButton:new Spawn.EnableBatchButton({
                collection:this.collection,
                css:{
                    "margin-left":"10px",
                    "color":"black"
                }
                ,selectedIds: this.selectedIds
            }).render(),
            disableJobButton:new Spawn.DisableBatchButton({
                collection:this.collection,
                css:{
                    "margin-left":"10px",
                    "color":"black"
                }
                ,selectedIds: this.selectedIds
            }).render(),
            selectJobButton:new Spawn.SelectBatchButton({
                collection:this.collection,
                css:{
                    "text-align":"left",
                    "width":"64px",
                    "margin-left":"20px"
                }
                ,selectedIds: this.selectedIds
            }).render(),
            deleteJobButton:new Spawn.DeleteBatchButton({
                collection:this.collection,
                css:{
                    "margin-left":"10px",
                    "color":"red"
                },
                selectedIds: this.selectedIds
            }).render(),
            settingsButton: new Spawn.DisplayDensityButton().render(),
            searchbox: new Spawn.TypeaheadBox({
                collection:this.collection,
                className:"search-query pull-right clearable",
                keys:["id","description","creator"],
                placeholder:"Search Jobs.."
            }).render()
        };
        if(!_.isUndefined(this.titleView)){
            this.views.titleView=this.titleView.render();
            this.views.titleView.$el.appendTo(this.$el);
        }
        if(!this.dontShowNewButton){
            this.views.newJobButton=new Spawn.NewJobButton({
            }).render();
            this.views.newJobButton.$el.appendTo(this.$el);
        }
        this.views.selectJobButton.$el.appendTo(this.$el);
        this.views.settingsButton.$el.appendTo(this.$el);
        this.views.kickJobButton.$el.appendTo(this.$el);
        this.views.stopJobButton.$el.appendTo(this.$el);
        this.views.killJobButton.$el.appendTo(this.$el);
        this.views.enableJobButton.$el.appendTo(this.$el);
        this.views.disableJobButton.$el.appendTo(this.$el);
        this.views.deleteJobButton.$el.appendTo(this.$el);
        this.views.searchbox.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.LogContentView = Backbone.View.extend({
    tagName:"pre",
    className:"span12 tasklog",
    events:{
    },
    initialize:function(options){
        //console.log("Initialized log content");
        this.appendMode=true;
        this.content = "";
        this.offset=-1;
        this.lastModified=-1;
        this.$el.css({
            "margin":"0px",
            "padding":"0px"
        });
    },
    render:function(){
        return this;
    },
    renderLoadingSpinner:function(){
        this.$el.html("<img src='/spawn2/spinner.gif'/> Loading...");
        this.offset=-1;
        this.lastModified=-1;
    },
    renderEmptyLog:function(){
        this.$el.css({color:"red"});
        this.$el.html("Log is empty.");
        this.offset=-1;
        this.lastModified=-1;
    },
    clearContent:function(){
        this.offset=-1;
        this.lastModified=-1;
        this.$el.css({color:"black"});
        this.$el.text("");
        this.content="";
    },
    updateContent:function(log){
        if(_.isEmpty(log)){
            return;
        }
        this.$el.css({color:"black"});
        if(log.offset<this.offset || log.lastModified<this.lastModified){
            this.clearContent();
            this.content=log.out;
        }
        else if(log.offset>this.offset){
            this.content=this.content+log.out;
        }
        else{
        }
        this.offset=log.offset;
        this.lastModified=log.lastModified;
        var isAtBottom = this.isAtBottom();//capture isAtBottom information before contnt update
        this.$el.text(this.content);
        if(isAtBottom){
            this.scrollToBottom();
        }
    },
    isAtBottom:function(){
        var scrollTop = this.$el.scrollTop();
        var innerHeight = this.$el.innerHeight();
        var scrollHeight = this.$el.get(0).scrollHeight;
        var isBottom =((scrollTop+innerHeight)>=(scrollHeight));
        return isBottom;
    },
    scrollToBottom:function(){
        var scrollHeight = this.$el.get(0).scrollHeight;
        var innerHeight = this.$el.innerHeight();
        var deltaHeight = scrollHeight-innerHeight;
        if(deltaHeight>0){
            this.$el.scrollTop(scrollHeight-innerHeight);
        }
    },
    close:function(){
        this.stopListening();
        this.clearContent();
        this.model.unset("log",{silent:true});
        this.$el.remove();
        return this;
    }
});
Spawn.TaskActionButtons = Backbone.View.extend({
    className:"form-inline",
    events:{
        "click button.kick-task":"handleKickTaskClick"
        ,"click button.stop-task":"handleStopTaskClick"
        ,"click button.kill-task":"handleKillTaskClick"
        ,"click button.revert-task":"handleRevertTaskClick"
    },
    template:_.template($("#taskActionButtonTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.hasRendered=false;
        this.$el.css({
            "display":"inline"
        });
        if(_.has(options,"taskNum")){
            this.taskNum=options.taskNum;
        }
        this.listenTo(this.collection,"change",this.render);
    },
    render:function(){
        this.$el.html(this.template({
            task:this.collection.get(this.taskNum).toJSON()
        }));
        return this;
    },
    getTask:function(){
        return this.collection.get(this.taskNum);
    },
    handleKickTaskClick:function(event){
        this.getTask().kick(true);
    },
    handleStopTaskClick:function(event){
        this.getTask().stop(true);
    },
    handleKillTaskClick:function(event){
        this.getTask().kill(true);
    },
    handleRevertTaskClick:function(event){
        var task = this.getTask();
        this.model.listBackups(task.id);
    }
});
Spawn.TaskLogControls = Backbone.View.extend({
    className:"pull-right",
    events:{
    },
    template:_.template($("#taskLogControlsTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.hasRendered=false;
        this.$el.css({
            "display":"inline"
        });
    },
    render:function(){
        this.$el.html(this.template());
        return this;
    },
    handleActiveClick:function(event){
        $(event.currentTarget).removeClass("active");
        return this;
    },
    handleInactiveClick:function(event){
        $(event.currentTarget).addClass("active");
        return this;
    },
    inactivateControlButtons:function(event){
        return this;
    }
});
Spawn.BalanceParamModel = Backbone.Model.extend({
    url:"/update/balance.params.get",
    defaults:{
        autoBalanceLevel:"",
        bytesMovedFullRebalance:"",
        hostAutobalanceIntervalMillis:"",
        jobAutobalanceIntervalMillis:"",
        tasksMovedFullRebalance:""
    },
    save:function(){
        var self=this;
        $.ajax({
            url: "/update/balance.params.set",
            type: "GET",
            data: {params:JSON.stringify(self.toJSON())},
            success: function(data){
                Alertify.log.info("Rebalance params saved successfully.");
            },
            error: function(resp){
                Alertify.log.error(resp.responseText);
            },
            dataType: "json"
        });
    }
});
Spawn.BalanceParamView = Backbone.View.extend({
    el:"div#balanceParams",
    className:"span12",
    events:{
        "click #saveBalanceParamButton":"handleSaveParamsClick",
        "click #resetBalanceParamButton":"handleResetParamsClick",
        "change input":"handleInputChange"
    },
    template:_.template($("#balanceParamsTemplate").html()),
    initialize:function(){
        _.bindAll(this);
        this.$el=$(this.el);
        this.listenTo(this.model,"change",this.render);
    },
    render:function(){
        this.$el=$(this.el);
        //this.$el.html("params");
        var html = this.template(this.model.toJSON());
        this.$el.html(html);
        this.show();
        return this;
    },
    handleInputChange:function(event){
        var input = $(event.currentTarget);
        this.model.set(input.attr("id"),input.val());
        return true;
    },
    handleSaveParamsClick:function(event){
        this.model.save();
    },
    handleResetParamsClick:function(event){
        this.model.fetch();
    },
    close:function(){
        this.$el.remove();
        this.show();
        return this;
    },
    show:function(){
        this.$el.show();
    },
    hide:function(){
        this.$el.hide();
        return this;
    }
});
Spawn.TaskLogView = Backbone.View.extend({
    className:"",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "overflow-y":"scroll",
            "font-size":"11px",
            "display":"inline-block",
            "width":"90%"
        });
        this.rolling=false;
        this.hasRendered=false;
        if(_.has(options,"taskNum")){
            this.taskNum=options.taskNum;
        }
        this.rollingTimeout=null;
        this.lastLogValue = null;
        this.stdout=true;
        this.lines=10;
        this.listenTo(this.collection,"reset",this.render);
    },
    task:function(){
        return this.collection.get(this.taskNum);
    },
    render:function(){
        var task = this.collection.get(this.taskNum);
        if(!this.hasRendered){
            this.views={
                logContent: new Spawn.LogContentView({
                    model:task
                }).render()
            };
            this.views.logContent.$el.appendTo(this.$el);
        }
        if(!this.hasRendered){
            this.roll();
        }
        this.hasRendered=true;
        return this;
    },
    isAtBottom:function(){
        var scrollTop = this.$el.scrollTop();
        var innerHeight = this.$el.innerHeight();
        var scrollHeight = this.$el.get(0).scrollHeight;
        var isBottom =((scrollTop+innerHeight)>=(scrollHeight));
        return isBottom;
    },
    scrollToBottom:function(){
        var scrollHeight = this.$el.get(0).scrollHeight;
        var innerHeight = this.$el.innerHeight();
        var deltaHeight = scrollHeight-innerHeight;
        if(deltaHeight>0){
            this.$el.scrollTop(scrollHeight-innerHeight);
        }
    },
    close:function(){
        this.stopRolling();
        if(!_.isUndefined(this.views.logContent)){
            this.views.logContent.close();
        }
        this.lastLogValue=null;
        this.stopListening();
        this.$el.remove();
        if(!_.isUndefined(this.taskNum) && this.collection.length>0){
            this.collection.get(this.taskNum).unset("log");
        }
        return this;
    },
    roll:function(){
        if(!this.rolling){
            this.rolling=true;
            this.views.logContent.clearContent();
            this.lastLogValue=null;
        }
        clearTimeout(this.rollingTimeout);
        this.rolling=true;
        var task = this.collection.get(this.taskNum);
        task.once("change:log",this.handleLogChange);
        if(!_.isEmpty(this.lastLogValue)){
            task.roll({
                offset:this.lastLogValue.offset,
                lines:this.lines,
                out:(this.stdout?1:0)
            });
        }
        else{
            task.roll({
                lines:this.lines,
                out:(this.stdout?1:0)
            });
        }
        if(this.rolling){
            this.rollingTimeout=setTimeout(this.roll,2000);
        }
        else{
            this.lastLogValue = null;
            clearTimeout(this.rollingTimeout);
        }
    },
    tail:function(){
        this.stopRolling();
        var isAtBottom = this.isAtBottom();
        this.views.logContent.clearContent();
        this.lastLogValue=null;
        var task = this.collection.get(this.taskNum);
        task.once("change:log",this.handleLogChange);
        task.tail({
            lines:this.lines,
            out:(this.stdout?1:0)
        });
    },
    head:function(){
        this.stopRolling();
        this.views.logContent.clearContent();
        this.lastLogValue=null;
        var task = this.collection.get(this.taskNum);
        task.once("change:log",this.handleLogChange);
        task.head({
            lines:this.lines,
            out:(this.stdout?1:0)
        });
    },
    stopRolling:function(){
        if(!_.isNull(this.rollingTimeout)){
            clearTimeout(this.rollingTimeout);
        }
        this.rollingTimeout=null;
        this.rolling=false;
    },
    handleLogChange:function(taskModel){
        var isAtBottom = this.isAtBottom();
        var log = taskModel.get("log");
        if(_.isUndefined(log)){
            this.views.logContent.renderLoadingSpinner();
            taskModel.once("change:log",this.handleLogChange);
        }
        else if(_.isNull(log)){
            this.views.logContent.renderEmptyLog();
            taskModel.once("change:log",this.handleLogChange);
        }
        else{
            this.views.logContent.updateContent(log);
        }
        if(isAtBottom){
            this.scrollToBottom();
        }
        if(this.rolling){
            this.lastLogValue=log;
        }
        else{
            this.lastLogValue=null;
        }
    }
});
Spawn.QuickJobView = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
        this.$el.css({
            "margin-left":"0px"
        });
        this.hostCollection=options.hostCollection;
    },
    render:function(){
        this.views={
            quickViewHeader:new Spawn.QuickViewHeader({
                model:this.model
            }).render(),
            taskTable:new Spawn.TaskTable({
                collection:this.collection,
                model:this.model
            }).render(),
            batchActions:new Spawn.TaskBatchActions({
                model:this.model,
                collection:this.collection
            }).render()
        };
        this.views.quickViewHeader.$el.appendTo(this.$el);
        this.views.batchActions.$el.appendTo(this.$el);
        this.views.taskTable.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.QuickViewHeader = Backbone.View.extend({
    className:"span12 top-bar navbar-inner form-inline",
    events:{
        "click i.icon-remove":"handleClose"
    },
    template:_.template($("#quickViewPanelHeader").html()),
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
    },
    render:function(){
        this.$el.html(this.template(this.model.toJSON()));
        return this;
    },
    handleClose:function(){
        Spawn.router.navigate("#jobs",{trigger:true});
    }
});
Spawn.CompactTaskTable = Backbone.View.extend({
    className:"compact-table",
    events:{
        "click tr":"rowClickHandler"
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
        this.$el.css({
            "margin-left":"0px",
            "cursor":"pointer",
            "display":"inline-block",
            "width":"10%"
        });
    },
    render:function(){
        this.views.header = new Table.HeaderView({
            collection: this.collection,
            templateId: "#compactTaskTableHeaderTemplate"
        }).render();
        this.views.body = new Table.BodyView({
            collection: this.collection,
            templateId: "#compactTaskTableRowTemplate"
        });
        this.views.bodyContainer=$("<div class='table-body-full-height'></div>").append(this.views.body.$el);
        this.views.header.$el.appendTo(this.$el);
        this.views.bodyContainer.appendTo(this.$el);
        this.hasRendered=true;
        this.$el.show();
        return this;
    },
    rowClickHandler:function(event){
        var target = $(event.target);
        if(target.is("tr.selectable") || target.is("tr.selectable td") || target.is("tr.selectable td input")){
            var id = $(event.currentTarget).data("id");
            var task = this.collection.get(id);
            var name = task.get("node");
            var jobUuid = task.get("jobUuid");
            var tableId = this.model.id;
            //alert(tableId+","+name);
        }
        return this;
    }
});
Spawn.QuickTaskView = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "margin-left":"0px"
        });
        if(_.has(options,"taskNum")){
            this.taskNum=options.taskNum;
        }
    },
    render:function(){
        this.views={
            quickViewHeader:new Spawn.QuickViewHeader({
                model:this.model
            }).render(),
            singleTaskView:new Spawn.SingleTaskView({
                model:this.model,
                collection:this.collection,
                taskNum:this.taskNum
            }).render()
        };
        this.views.quickViewHeader.$el.appendTo(this.$el);
        this.views.singleTaskView.$el.appendTo(this.$el);
        return this;
    },
    close:function(){
        this.views.singleTaskView.close();
        this.$el.remove();
        return this;
    }
});
Spawn.SingleTaskView = Backbone.View.extend({
    className:"span12",
    events:{
        "click tr":"renderLog"
        ,"click button#rollButton:not(.active)":"handleRollLog"
        ,"click button#rollButton.active":"handleStopRollLog"
        ,"click button#tailButton":"handleTailClick"
        ,"click button#headButton":"handleHeadClick"
        ,"change input#linesInput":"handleLineInputChange"
        ,"keypress input#linesInput":"handleLineInputKeypress"
        ,"click button#stdoutButton:not(.active)":"handleStdoutClick"
        ,"click button#stderrButton:not(.active)":"handleStderrClick"
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "margin-left":"0px"
        });
        if(_.has(options,"taskNum")){
            this.taskNum=options.taskNum;
        }
        this.hasRendered=false;
        this.lines=10;
        this.stdout=true;
    },
    render:function(){
        if(!this.hasRendered){
            this.views={
                singleTaskActions:new Spawn.SingleTaskActions({
                    model:this.model,
                    taskNum:this.taskNum,
                    collection:this.collection
                }),
                taskTable: new Spawn.CompactTaskTable({
                    collection:this.collection,
                    model:this.model
                }).render(),
                logView:new Spawn.TaskLogView({
                    collection:this.collection,
                    model:this.model,
                    taskNum:this.taskNum
                })
            };
            this.views.singleTaskActions.$el.appendTo(this.$el);
            this.views.taskTable.$el.appendTo(this.$el);
            this.views.logView.$el.appendTo(this.$el);
        }
        else{
            var logViewHeight = this.views.logView.$el.height();
            if(!_.isUndefined(this.views.logView)){
                this.views.logView.close();
            }
            this.views.logView=new Spawn.TaskLogView({
                collection:this.collection,
                model:this.model,
                taskNum:this.taskNum
            }).render();
            this.views.logView.$el.height(logViewHeight);
            this.views.logView.$el.appendTo(this.$el);
            this.views.singleTaskActions.taskNum=this.taskNum;
            this.views.singleTaskActions.render();
        }
        this.lastAction="roll";
        this.hasRendered=true;
        return this;
    },
    renderLog:function(event){
        this.taskNum=$(event.currentTarget).data("id");
        if(!_.isUndefined(this.views.logView)){
            this.views.logView.close();
        }
        var windowLoc = window.location.hash;
        var lastIndex = windowLoc.lastIndexOf("/");
        var taskLoc = windowLoc.substring(lastIndex);
        var newWindowLoc = windowLoc.replace(taskLoc,"/"+this.taskNum);
        Spawn.router.navigate(newWindowLoc,{trigger:false});
        this.render();
        return this;
    },
    rerunLastAction:function(){
        if(_.isEqual(this.lastAction,"tail")){
            this.handleTailClick();
        }
        else if(_.isEqual(this.lastAction,"head")){
            this.handleHeadClick();
        }
        else if(_.isEqual(this.lastAction,"roll")){
            this.handleRollLog();
        }
        else{
        }
        return this;
    },
    handleStdoutClick:function(){
        this.stdout=true;
        this.views.logView.stopRolling();
        this.rerunLastAction();
    },
    handleStderrClick:function(){
        this.stdout=false;
        this.views.logView.stopRolling();
        this.rerunLastAction();
    },
    updateLineNumber:function(lines){
        var value = Number(lines);
        if(_.isNaN(value) || _.isEqual(value,0)){
            Alertify.log.error(lines + " is invalid, keeping "+this.lines+" instead.");
            return false;
        }
        else{
            this.lines=value;
            this.views.logView.lines=this.lines;
            this.rerunLastAction();
            return true;
        }
    },
    handleLineInputChange:function(event){
        var input = $(event.currentTarget);
        if(!this.updateLineNumber(input.val())){
            input.val(this.lines);
        }
        return this;
    },
    handleLineInputKeypress:function(event){
        if(_.isEqual(event.which,13)){
            var input = $(event.currentTarget);
            if(!this.updateLineNumber(input.val())){
                input.val(this.lines);
            }
        }
        return this;
    },
    close:function(){
        if(!_.isUndefined(this.views.logView)){
            this.views.logView.close();//.$el.remove();
            this.views.logView=undefined;
        }
        this.$el.remove();
        return this;
    },
    handleRollLog:function(event){
        this.views.logView.stopRolling();
        this.views.logView.stdout=this.stdout;
        this.views.logView.roll();
        if(!_.isUndefined(event)){
            $(event.currentTarget).addClass("active");
        }
        this.lastAction="roll";
    },
    handleStopRollLog:function(event){
        this.views.logView.stopRolling();
        if(!_.isUndefined(event)){
            $(event.currentTarget).addClass("active");
            event.preventDefault();//to keep active on #rollButton
            event.stopImmediatePropagation();
        }
        this.lastAction=undefined;
    },
    handleTailClick:function(event){
        this.views.logView.stdout=this.stdout;
        this.views.logView.tail({
            lines:this.lines,
            out:this.stdout
        });
        this.lastAction="tail";
    },
    handleHeadClick:function(event){
        this.views.logView.stdout=this.stdout;
        this.views.logView.head({
            lines:this.lines,
            out:this.stdout
        });
        this.lastAction="head";
    }
});
Spawn.TaskTable = Backbone.View.extend({
    className:"span12",
    events:{
        "click tr":"rowClickHandler",
        "click a":"anchorClickHandler"
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
        this.$el.css({
            "margin-left":"0px"
        });
    },
    render:function(){
        this.views.header = new Table.HeaderView({
            collection: this.collection,
            templateId: "#taskTableHeaderTemplate"
        }).render();
        this.views.body = new Table.BodyView({
            collection: this.collection,
            templateId: "#taskTableRowTemplate"
        });
        this.views.bodyContainer=$("<div class='table-body-full-height'></div>").append(this.views.body.$el);
        this.views.header.$el.appendTo(this.$el);
        this.views.bodyContainer.appendTo(this.$el);
        this.hasRendered=true;
        this.$el.show();
        return this;
    },
    rowClickHandler:function(event){
        var target = $(event.target);
        if(target.is("tr.selectable") || target.is("tr.selectable td") || target.is("tr.selectable td input")){
            var id = $(event.currentTarget).data("id");
            var task = this.collection.get(id);
            var name = task.get("id");
            if(task.isSelected){
                task.unselect();
            }
            else{
                task.select();
            }
        }
        return this;
    },
    anchorClickHandler:function(event){
        var anchor = $(event.currentTarget);
        if(anchor.is("a[data-bypass]")){
            window.open(anchor.attr("href"),'_blank');
        }
        else{
            Spawn.router.navigate(anchor.attr("href"),{trigger:true});
        }
    }
});
Spawn.MoreJobActionButton = Backbone.View.extend({
    className:"btn-mini btn-group",
    events:{
        "click a#cloneAction":"handleCloneClick"
        ,"click a#rebalanceAction":"handleRebalanceClick"
        ,"click a#enableAction":"handleEnableClick"
        ,"click a#disableAction":"handleDisableClick"
        ,"click a#checkDirsAction":"handleCheckDirsClick"
        ,"click a#fixDirsAction":"handleFixDirsClick"
        ,"click a#expandAction":"handleExpandClick"
        ,"click a#revertAction":"handleRevertClick"
    },
    template:_.template($("#jobMoreActionButtonTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.$el.css({
            "margin-left":"10px"
        });
    },
    render:function(){
        this.$el.html(this.template(this.model.toJSON()));
        return this;
    },
    handleCloneClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        Spawn.router.navigate("#jobs/"+this.model.id+"/clone/conf",{trigger:true});
    },
    handleRebalanceClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        var self=this;
        var totalNodes = Math.max(1, self.model.attributes.nodes);
        var averageTaskSize = Math.floor(self.model.attributes.bytes/totalNodes);
        Alertify.dialog.prompt("Enter max number of tasks to move, or leave blank to use default. Average task size for this job is " + convertToDFH(averageTaskSize), function(tasks) {
        	self.model.rebalance(tasks);
        });
    },
    handleEnableClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.enable();
    },
    handleDisableClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.disable();
    },
    handleCheckDirsClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.checkDirs();
    },
    handleFixDirsClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.fixDirs();
    },
    handleExpandClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.expand();
    },
    handleRevertClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.listBackups();
    }
});
Spawn.KickJobButton = Spawn.ActionButton.extend({
    events:{
        "click":"kick"
    },
    kick:function(){
        this.model.kick();
        return this;
    },
    text:"Kick"
});
Spawn.StopJobButton = Spawn.ActionButton.extend({
    events:{
        "click":"stop"
    },
    stop:function(){
        this.model.stop();
        return this;
    },
    text:"Stop"
});
Spawn.KillJobButton = Spawn.ActionButton.extend({
    events:{
        "click":"kill"
    },
    kill:function(){
        this.model.kill();
        return this;
    },
    text:"Kill"
});
Spawn.QueryJobButton = Spawn.ActionButton.extend({
    tagName:"button",
    className:"btn btn-mini",
    events:{
        "click":"query"
    },
    initialize:function(options){
        _.bindAll(this);
        if(!_.isUndefined(options.css)){
            this.$el.css(_.extend({
                "display":"inline-block",
                "vertical-align":"top",
                "display":"-moz-inline-stack"
            },options.css));
        }
        this.listenTo(this.model,"change:canQuery",this.render);
    },
    render:function(){
        if(this.model.get("canQuery")){
            this.$el.show();
        }
        else{
            this.$el.hide();
        }
        this.$el.html("Query");
        return this;
    },
    query:function(){
        this.model.query();
        return this;
    }
});
Spawn.SaveJobButton = Backbone.View.extend({
    tagName:"button",
    className:"btn btn-mini btn-save-job",
    events:{
        "click":"save"
    },
    initialize:function(options){
        _.bindAll(this);
        if(!_.isUndefined(options.css)){
            this.$el.css(_.extend({
                "display":"inline-block",
                "vertical-align":"top",
                "display":"-moz-inline-stack"
            },options.css));
        }
        this.listenTo(this.model,"user_edit",this.handleUserEdit);
        this.commit = options.commit;
        this.model.commit = null;
    },
    render:function(){
        this.$el.html(this.commit ? "Commit" : "Save");
        if(this.model.userEditted){
            this.$el.addClass("btn-primary");
        }
        else{
            this.$el.removeClass("btn-primary");
        }
        return this;
    },
    handleUserEdit:function(event){
		this.$el.addClass("btn-primary");
		this.model.userEditted=true;
    },
    save:function(){
    	if (this.commit){
    	    var model = this.model;
    	    var el = this.$el;
    		Alertify.dialog.prompt("Enter a commit message: ",function(str){
            	model.commit = str;
            	model.save();
        		$('.btn-save-job').removeClass("btn-primary");
        		model.userEditted=false;
        		return this;
        	});
    	}
    	else
    	{
    		this.model.commit = null;
    		this.model.save();
        	$('.btn-save-job').removeClass("btn-primary");
        	this.listenTo(this.model,"user_edit",this.handleUserEdit);
        	return this;
    	}        
    },
    handleJobChange:function(changeModel,jqXHR){
        if(_.keys(_.omit(this.model.changed,["state"])).length>0){
			this.$el.addClass("btn-primary");
        }
        else{
			this.$el.removeClass("btn-primary");
        }
    }
});
Spawn.DiscardJobButton = Backbone.View.extend({
    tagName:"button",
    className:"btn btn-mini",
    events:{
        "click":"discard"
    },
    initialize:function(options){
        _.bindAll(this);
        if(!_.isUndefined(options.css)){
            this.$el.css(_.extend({
                "display":"inline-block",
                "vertical-align":"top",
                "display":"-moz-inline-stack"
            },options.css));
        }
    },
    render:function(){
        this.$el.html("Discard");
        return this;
    },
    discard:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        Spawn.router.navigate("#jobs",{trigger:true});
    }
});
Spawn.DeleteJobButton = Spawn.ActionButton.extend({
    events:{
        "click":"delete"
    },
    delete:function(){
        var description = this.model.get("description");
        var model = this.model;
        Alertify.dialog.confirm( ("Are you sure you would like to DELETE job: "+description), function (e) {
            model.delete();
        });
        return this;
    },
    text:"Delete"
});
Spawn.NewJobButtons = Backbone.View.extend({
    className:"top-actions pull-right",
    events:{
    },
    initialize:function(options){
        this.$el.css({
            "display":"inline"
        });
    },
    render:function(){
        this.views={
            saveButton: new Spawn.SaveJobButton({
                css:{
                    "margin-left":"10px"
                },
                model:this.model
            }).render()
            ,discardButton: new Spawn.DiscardJobButton({
                css:{
                    "margin-left":"10px"
                }
            }).render()
        };
        this.views.saveButton.$el.appendTo(this.$el);
        this.views.discardButton.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.JobActionButtons = Backbone.View.extend({
    className:"top-actions pull-right",
    events:{
    },
    initialize:function(options){
        this.$el.css({
            "display":"inline"
        });
    },
    render:function(){
        //this.$el.empty();
        this.views={
            kickButton: new Spawn.KickJobButton({
                css:{
                    "color":"green"
                    ,"margin-left":"10px"
                }
                ,model:this.model
            }).render()
            ,stopButton: new Spawn.StopJobButton({
                css:{
                    "color":"red"
                    ,"margin-left":"10px"
                }
                ,model:this.model
            }).render()
            ,killButton: new Spawn.KillJobButton({
                css:{
                    "color":"red"
                    ,"margin-left":"10px"
                }
                ,model:this.model
            }).render()
            ,queryButton: new Spawn.QueryJobButton({
                css:{
                    "color":"blue"
                },
                model:this.model
            }).render()
            ,saveButton: new Spawn.SaveJobButton({
                css:{
                    "margin-left":"10px"
                },
                model:this.model
            }).render()
            ,commitButton: new Spawn.SaveJobButton({
                css:{
                    "margin-left":"10px"
                },
                model:this.model,
                commit:true
            }).render()
            ,deleteButton: new Spawn.DeleteJobButton({
                css:{
                    "color":"red"
                    ,"margin-left":"10px"
                },
                model:this.model
            }).render()
            ,moreActionButton: new Spawn.MoreJobActionButton({
                model:this.model
            }).render()
        };
        this.views.queryButton.$el.appendTo(this.$el);
        this.views.kickButton.$el.appendTo(this.$el);
        this.views.stopButton.$el.appendTo(this.$el);
        this.views.killButton.$el.appendTo(this.$el);
        this.views.deleteButton.$el.appendTo(this.$el);
        this.views.moreActionButton.$el.appendTo(this.$el);
        this.views.saveButton.$el.appendTo(this.$el);
        this.views.commitButton.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.TabsView = Backbone.View.extend({
    tagName:"ul",
    className:"nav nav-tabs",
    events:{
        "click li.active":"handleActiveClick",
        "click li:not(.active)":"handleInactiveClick"
    },
    initialize:function(options){
        this.$el.css({
            "display":"inline"
        });
        if(_.has(options,"template")){
            this.template=options.template;
        }
        this.hasRendered=false;
    },
    render:function(){
        var html = this.template(this.model);
        this.$el.html(html);
        this.hasRendered=true;
        return this;
    },
    handleActiveClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        return this;
    },
    handleInactiveClick:function(event){
        var item = $(event.currentTarget);
        item.siblings().removeClass("active");
        item.addClass("active");
        return this;
    },
    handleAnchorClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        var anchor = $(event.currentTarget);
        var href = anchor.attr("href");
        return this;
    }
});
Spawn.JobStatusBarView = Backbone.View.extend({
    events:{
    },
    template:_.template($("#jobStatusBarTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.views={};
        this.$el.css({
            "display":"inline",
            "background-color":"transparent"
        });
        if(_.has(options,"template")){
            this.template=options.template;
        }
        this.listenTo(this.model,"change",this.render);
    },
    render:function(){
        if(!this.model.isNew()){
            var html = this.template({
                job:this.model.toJSON()
            });
            this.$el.html(html);
        }
        return this;
    }
});
Spawn.JobActionView = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        this.$el.css({
            "display":"inline"
        });
        this.listenTo(this.model,"change:id",this.render);
    },
    render:function(){
        this.$el.empty();
        if(this.model.isNew()){
            this.views={
                actionButtons: new Spawn.NewJobButtons({
                    model:this.model
                }).render(),
                breadcrumb: new Spawn.BreadcrumbView({
                    template:_.template($("#newJobDetailBreadcrumbsTemplate").html()),
                    model:this.model.toJSON()
                }).render()
            };
            this.views.actionButtons.$el.appendTo(this.$el);
            this.views.breadcrumb.$el.appendTo(this.$el);
        }
        else{
            this.views={
                statusBar: new Spawn.JobStatusBarView({
                    model:this.model
                }).render(),
                actionButtons: new Spawn.JobActionButtons({
                    model:this.model
                }).render(),
                breadcrumb: new Spawn.BreadcrumbView({
                    template:_.template($( (!this.model.isNew()?"#jobDetailBreadcrumbsTemplate":"#newJobDetailBreadcrumbsTemplate") ).html()),
                    model:this.model.toJSON()
                }).render()
            };
            this.views.breadcrumb.$el.appendTo(this.$el);
            this.views.statusBar.$el.appendTo(this.$el);
            this.views.actionButtons.$el.appendTo(this.$el);
        }
        return this;
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        return this;
    }
});
Spawn.CodeMirrorView = Backbone.View.extend({
    className:"span12 container",
    events:{
        "change":"handleEditorChange"
        ,"keypress":"handleEditorChange"
        ,"keydown":"handleEditorChange"
    },
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
            ,"height":"82%"
            ,"overflow":"scroll"
            ,"border":"1px solid"
        });
        this.hasRendered=false;
        this.keyName = (_.has(options,"keyName")?options.keyName:"config");
        this.listenTo(this.model,"change:"+this.keyName,this.handleConfigChange);
    },
    render:function(){
        var keyName = this.keyName;
        this.views = {
            editor: CodeMirror(this.el, {
                value: this.model.get(keyName) || " "
                ,mode:  "javascript"
                ,autofocus:true
                ,lineNumbers:true
            })
        }
        this.views.editor.setSize("100%","100%");
        this.refresh();
        return this;
    },
    handleConfigChange:function(event){
        this.views.editor.setValue(this.model.get(this.keyName));
        this.refresh();
        return this;
    },
    handleEditorChange:function(event){
        event.stopImmediatePropagation();
        var content = this.views.editor.getValue();
        this.model.set(this.keyName,content);
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        return this;
    },
    refresh:function(){
        this.views.editor.setSize("100%","100%");
        this.views.editor.refresh();
        return this;
    }
});
Spawn.JobParameterView = Backbone.View.extend({
    className:"span12 skinnyWell",
    events:{
        "change input":"handleInputChange"
    },
    template:_.template($("#jobParamTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
            ,"min-height":"0px"
        });
        this.hasRendered=false;
        this.listenTo(this.model,"change:parameters",this.render);
        this.listenTo(this.model,"change:config",this.render);
    },
    render:function(){
        var cookie = $.cookie("hideParam");
        var html = this.template({
            hidden:_.isEqual(cookie,1),
            job:this.model.toJSON()
        });
        this.$el.html(html);
        if(!_.isUndefined(cookie) && _.isEqual(cookie,0)){
            this.$el.find("div#paramBox").show();
        }
        else{
            this.$el.find("div#paramBox").hide();
        }
        this.trigger("app:refresh");
        return this;
    },
    handleInputChange:function(event){
        var input = $(event.currentTarget);
        var param = _.findWhere(this.model.get("parameters"),{name:input.attr("name")});
        if(!_.isEmpty(param)){//_.isNumber(index)){
            param.value=input.val();
        }
    }
});
Spawn.JobTaskTableView = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        this.$el.css({
            "margin-left":"0px"
        });
    },
    render:function(){
        this.views={
            taskTable:new Spawn.TaskTable({
                collection:this.collection,
                model:this.model
            }).render(),
            batchActions:new Spawn.TaskBatchActions({
                model:this.model,
                collection:this.collection
            }).render()
        };
        this.views.batchActions.$el.appendTo(this.$el);
        this.views.taskTable.$el.appendTo(this.$el);
        return this;
    },
    close:function(){
        this.$el.remove();
        return this;
    }
});
Spawn.JobExpandedConfView = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
        });
        this.views={};
    },
    render:function(){
        this.views = {
            editor: new Spawn.AceEditorView({
                model:this.model
                ,keyName:"expandedConfig"
                ,readOnly:true
            }).render()
        };
        this.views.editor.$el.appendTo(this.$el);
        return this;
    },
    refresh:function(){
    },
    close:function(){
    }
});
Spawn.JobConfView = Backbone.View.extend({
    className:"span12",
    events:{
        "click #hideParamLink":"handleHideParamClick"
        ,"click #validateSourceLink":"handleValidateClick"
        ,"change input":"handleInputChange"
        ,"change select":"handleInputChange"
    },
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
        });
        this.hasRendered=false;
        if(_.isUndefined($.cookie("hideParam"))){
            $.cookie("hideParam",0);
        }
        if(this.model.isNew() || !_.isUndefined(this.model.cloneId)){
            this.template=_.template($("#newJobConfigurationTemplate").html());
        }
        else{
            this.template=_.template($("#jobConfigurationTemplate").html());
        }
    },
    handleInputChange:function(event){
        var input = $(event.currentTarget);
        var name = input.attr("name");
        this.model.set(name,input.val());
    },
    handleValidateClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.validateSource();
    },
    render:function(){
        var data = {
            job:this.model.toJSON(),
            commands: _.pluck(Spawn.commandCollection.toJSON(),"name"),//_.keys(Spawn.commandCollection.toJSON()),
            isClone: !_.isUndefined(this.model.cloneId)
        };
        this.$el.html(this.template(data));
        this.views = {
            editor: new Spawn.AceEditorView({
                model:this.model
                ,keyName:"config"
            }).render(),
            parameters: new Spawn.JobParameterView({
                model:this.model
            }).render()
        };
        this.views.editor.$el.appendTo(this.$el);
        this.views.parameters.$el.appendTo(this.$el);
        this.listenTo(this.views.parameters,"app:refresh",this.refresh);
        this.refresh();
        return this;
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        return this;
    },
    handleHideParamClick:function(event){
        if(!_.isUndefined(event)){
            event.preventDefault();
            event.stopImmediatePropagation();
        }
        var hide = $.cookie("hideParam");
        if(_.isEqual(hide,0)){
            this.views.parameters.$el.find("div#paramBox").hide();
            $.cookie("hideParam",1);
            this.$el.find("#hideParamLink").html("Show Parameters");
        }
        else{
            this.views.parameters.$el.find("div#paramBox").show();
            $.cookie("hideParam",0);
            this.$el.find("#hideParamLink").html("Hide Parameters");
        }
        this.refresh();
    },
    refresh:function(){
        var height = this.$el.height();
        var parentTop = this.$el.offset().top;
        var childTop = this.views.editor.$el.offset().top;
        var delta = childTop-parentTop;
        var paramHeight = 0;
        if(!this.views.parameters.$el.is(":hidden")){
            paramHeight=(!_.isEmpty(this.model.get("parameters"))?this.views.parameters.$el.height():0);
        }
        this.views.editor.$el.height(height-delta-paramHeight-10);
        this.views.editor.views.editor.resize();
        return this;
    }
});
Spawn.JobSettingsView = Backbone.View.extend({
    className:"span12",
    events:{
        "change input":"handleInputChange"
    },
    template:_.template($("#jobSettingTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
            ,"overflow-y": "scroll"
            ,"overflow-x": "hidden"
        });
        this.hasRendered=false;
    },
    handleInputChange:function(event){
        var input = $(event.currentTarget);
        var name = input.attr("name");
        var value = (_.isEqual(input.attr("type"),"checkbox")?input.is(":checked"):input.val());
        if(name.startsWith("qc_")){
            var queryConfig = this.model.get("queryConfig");
            name=name.replace("qc_","");
            queryConfig[name]=value;
            this.model.set("queryConfig",queryConfig);
        }
        else{
            this.model.set(name,value);
        }
    },
    render:function(){
        this.$el.html(this.template({
            job:this.model.toJSON(),
            isNew: this.model.isNew()
        }));
        return this;
    },
    close:function(){
        this.$el.remove();
        return this;
    }
});
Spawn.JobHistoryView = Backbone.View.extend({
    className:"span12",
    template:_.template($("#jobHistoryTemplate").html()),
    events:{
        "click button.conf-diff":"handleDiffClick"
        ,"click button.conf-view":"handleViewClick"
        ,"click button.conf-load":"handleLoadClick"
    },
    
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
        });
        this.hasRendered=false;
    },

    handleDiffClick:function(event){
    	Spawn.jobView.model.runConfigCommand("config.diff", this.model.id, event.currentTarget.dataset.commit);
    },
    handleViewClick:function(event){
    	Spawn.jobView.model.runConfigCommand("config.view", this.model.id, event.currentTarget.dataset.commit);
    },
    handleLoadClick:function(event){
    	Spawn.jobView.model.runConfigCommand("config.view", this.model.id, event.currentTarget.dataset.commit, true);
    },
    render:function(){
    	var html = this.template({history:this.$el.data("history")});
    	this.$el.html(html);
        this.views = {
            editor: new Spawn.AceEditorView({
                model:this.model
                ,keyName:"historyConfig"
                ,readOnly:true
            }).render()
        };
        this.views.editor.$el.appendTo(this.$el);
        this.refresh();
        return this;
    },
    close:function(){
        this.$el.remove();
    },    
    refresh:function(){
        this.views.editor.views.editor.resize();
        return this;
    }
});
Spawn.JobAlertsView = Backbone.View.extend({
    className:"span12",
    events:{
        "click button#addAlertButton":"handleAddAlertClick"
        ,"change input":"handleInputChange"
        ,"change select":"handleInputChange"
        ,"click button.close":"handleCloseClick"
    },
    template:_.template($("#jobAlertTabTemplate").html()),
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px"
        });
        this.hasRendered=false;
        this.listenTo(this.model,"change:alerts",this.render);
    },
    render:function(){
        var html = this.template(this.model.toJSON());
        this.$el.html(html);
        return this;
    },
    close:function(){
        this.$el.remove();
        return this;
    }
    ,handleCloseClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        var input = $(event.currentTarget);
        var tr = input.closest("tr.row");
        var alertIndex = tr.data("index");
        var alerts = this.model.get("alerts");
        alerts.splice(alertIndex,1);
        this.model.set({
            alerts:alerts
        },{trigger:true});
        this.model.trigger("change:alerts",this.model);
        this.model.trigger("user_edit");
    }
    ,handleInputChange:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        var input = $(event.currentTarget);
        var tr = input.closest("tr.row");
        var alertIndex = tr.data("index");
        var alerts = this.model.get("alerts");
        var alert = alerts[alertIndex];
        alert.email = tr.find("input[name='email']").first().val();
        var typeInput = tr.find("select[name='type']").first();
        alert.type = parseInt(typeInput.val());
        var timeoutInput = tr.find("input[name='timeout']").first();
        var timeout = timeoutInput.val();
        alert.timeout = (_.isEqual(timeout,"-")?-1:timeout);
        if(parseInt(alert.type)>1){
            if(_.isEqual(timeoutInput.val(),"-")){
                timeoutInput.val("");
            }
            timeoutInput.removeAttr("disabled");
        }
        else{
            timeoutInput.val("-");
            timeoutInput.attr("disabled","1");
        }
        this.model.set({
            alerts:alerts
        });
        this.model.trigger("user_edit");
    }
    ,handleSelectChange:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        var input=$(event.currentTarget);
    }
    ,handleAddAlertClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        var alerts = this.model.get("alerts");
        alerts.push({
            email:"",
            type:0,
            timeout:-1,
            lastAlertTime:-1
        });
        this.model.set({
            alerts:alerts
        },{trigger:true});
        this.model.trigger("change:alerts",this.model);
        this.model.trigger("user_edit");
    }
});
Spawn.TreeForceDirectedGraphView = Backbone.View.extend({
	initialize:function(){
		_.bindAll(this);
	},
	render:function(){
		var w = 960,
            h = 500,
            node,
            link,
            root;

        var force = d3.layout.force()
            .on("tick", this.tick)
            .size([w, h]);

        var vis = d3.select(this.$el.get(0)).append("svg")
            .attr("width", w)
            .attr("height", h);

		this.update();
        return this;
	},
	// Returns a list of all nodes under the root.
	flatten:function(root) {
	  var nodes = [], i = 0;

	  function recurse(node) {
		if (node.children) node.children.forEach(recurse);
		if (!node.id) node.id = ++i;
		nodes.push(node);
	  }

	  recurse(root);
	  return nodes;
	},
	// Toggle children on click.
	click: function(d) {
	  if (d.children) {
		d._children = d.children;
		d.children = null;
	  } else {
		d.children = d._children;
		d._children = null;
	  }
	  this.update();
	},
	// Color leaf nodes orange, and packages white or blue.
	color:function(d) {
	  return d._children ? "#3182bd" : d.children ? "#c6dbef" : "#fd8d3c";
	},
	tick: function() {
	  link.attr("x1", function(d) { return d.source.x; })
		  .attr("y1", function(d) { return d.source.y; })
		  .attr("x2", function(d) { return d.target.x; })
		  .attr("y2", function(d) { return d.target.y; });

	  node.attr("cx", function(d) { return d.x; })
		  .attr("cy", function(d) { return d.y; });
	},
	update: function() {
		var w = 960,
            h = 500,
            node,
            link,
            root;

        var force = d3.layout.force()
            .on("tick", this.tick)
            .size([w, h]);

        var vis = d3.select(this.$el.get(0)).append("svg")
            .attr("width", w)
            .attr("height", h);
	  var root = this.model.toJSON();
	  var nodes = this.flatten(root),
		  links = d3.layout.tree().links(nodes);

	  // Restart the force layout.
	  force.nodes(nodes)
		  .links(links)
		  .start();

	  // Update the links…
	  link = vis.selectAll("line.link")
		  .data(links, function(d) { return d.target.id; });

	  // Enter any new links.
	  link.enter().insert("svg:line", ".node")
		  .attr("class", "link")
		  .attr("x1", function(d) { return d.source.x; })
		  .attr("y1", function(d) { return d.source.y; })
		  .attr("x2", function(d) { return d.target.x; })
		  .attr("y2", function(d) { return d.target.y; });

	  // Exit any old links.
	  link.exit().remove();

	  // Update the nodes…
	  node = vis.selectAll("circle.node")
		  .data(nodes, function(d) { return d.id; })
		  .style("fill", this.color);

	  // Enter any new nodes.
	  node.enter().append("svg:circle")
		  .attr("class", "node")
		  .attr("cx", function(d) { return d.x; })
		  .attr("cy", function(d) { return d.y; })
		  .attr("r", function(d) { return Math.sqrt(d.size) / 10 || 4.5; })
		  .style("fill", this.color)
		  .on("click", this.click)
		  .call(force.drag);

	  // Exit any old nodes.
	  node.exit().remove();
	}
});
Spawn.ForceDirectedGraphView = Backbone.View.extend({
	initialize:function(){

	},
	render:function(){
		var width = 960,height = 500;
		var color = d3.scale.category20();
		var force = d3.layout.force()
			.charge(-120)
			.linkDistance(30)
			.size([width, height]);
		var svg = d3.select(this.$el.get(0)).append("svg")
			.attr("width", width)
			.attr("height", height);
		var graph = this.model.toJSON();
		force.nodes(graph.nodes)
			.links(graph.links)
			.start();
		var link = svg.selectAll(".link")
			.data(graph.links)
			.enter().append("line")
			.attr("class", "link")
			.style("stroke-width", function(d) { return Math.sqrt(d.value); });
		var node = svg.selectAll(".node")
			.data(graph.nodes)
			.enter().append("circle")
			.attr("class", "node")
			.attr("r", 5)
			.style("fill", function(d) { return color(d.group); })
			.call(force.drag);
		node.append("title")
			.text(function(d) { return d.name; });
		force.on("tick", function() {
			link.attr("x1", function(d) { return d.source.x; })
				.attr("y1", function(d) { return d.source.y; })
				.attr("x2", function(d) { return d.target.x; })
				.attr("y2", function(d) { return d.target.y; });
			node.attr("cx", function(d) { return d.x; })
				.attr("cy", function(d) { return d.y; });
		});
		return this;
	}
});
Spawn.TreeDependencyGraphView = Backbone.View.extend({
	initialize:function(){

	},
	render:function(){
        var diameter = 960;
        var tree = d3.layout.tree()
			.size([360, diameter / 2 - 120])
			.separation(function(a, b) {
				return (a.parent == b.parent ? 1 : 2) / a.depth;
			});
        var diagonal = d3.svg.diagonal.radial()
            .projection(function(d) {
                var x = (d.x/180.0)*Math.PI;
                return [d.y, (d.x / 180) * Math.PI];
            });
        var svg = d3.select(this.$el.get(0)).append("svg")
			.attr("width", diameter)
			.attr("height", diameter - 150)
			.append("g")
			.attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
		var nodes = tree.nodes(this.model.toJSON());
		var links = tree.links(nodes);
		var link = svg.selectAll(".link")
			.data(links)
			.enter().append("path")
			.attr("class", "link")
			.attr("d", diagonal);
		var node = svg.selectAll(".node")
			.data(nodes)
			.enter().append("g")
			.attr("class", "node")
			.attr("transform", function(d) {
				return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")";
			});
		node.append("circle")
			.attr("r", 4.5);
		node.append("text")
			.attr("dy", ".31em")
			.attr("text-anchor", function(d) {
				return d.x < 180 ? "start" : "end";
			})
			.attr("transform", function(d) {
				return d.x < 180 ? "translate(8)" : "rotate(180)translate(-8)";
			})
			.text(function(d) {
				return d.name;
			});
		return this;
	}
});
Spawn.ForceDirectedGraphModel = Backbone.Model.extend({
	initialize:function(opts){
		_.bindAll(this);
		this.jobId=opts.jobId;
	},
	url:function(){
		return "/job/dependencies.list?id="+this.jobId;
	},
	parse:function(data){
		var nodeMap = {}, nodeList=[];
		_.each(data.nodes,function(node,idx){
			nodeMap[node.name]=nodeList.length;
			nodeList.push({
				name:node.name,
				group:1
			});
		});
		var edges = [];
		_.each(data.dependencies,function(edge){
			edges.push({
				source:nodeMap[edge.dependency],
				target:nodeMap[edge.dependent],
				value:1
			});
		});
		return {
			nodes:nodeList,
			links:edges
		};
	}
});
Spawn.TreeGraphModel = Backbone.Model.extend({
	initialize:function(opts){
		_.bindAll(this);
		this.jobId=opts.jobId;
	},
	url:function(){
		return "/job/dependencies.list?id="+this.jobId;
	},
	parse:function(data){
		var nodes = {};
		_.each(data.nodes,function(node){
			nodes[node.name]=node;
		});
		var edges = {};
		_.each(data.dependencies,function(edge){
			if(_.isUndefined(edges[edge.dependency])){
				edges[edge.dependency]=[edge.dependent];
			}
			else{
				edges[edge.dependency].push(edge.dependent);
			}
		});
		var graph = this.buildGraph(data.flow_id,nodes,edges);
		return graph;
	},
	buildGraph:function(nodeId,nodes,edges){
		var childrenIds = edges[nodeId];
		var childrenNodes = [],self=this;
		_.each(childrenIds,function(child){
			var childNode = self.buildGraph(child,nodes,edges);
			childrenNodes.push(childNode);
		});
		return {
			name:nodeId.substring(0,10),
			children: childrenNodes,
			size:1
		};
	}
});
Spawn.JobDepView = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.$el.css({
            "margin-left":"0px",
            "width":"960px",
            "height":"860px"
        });
        this.hasRendered=false;
        this.listenTo(this.model,"change",this.render);
    },
    render:function(){
        this.$el.html("");
        this.views={
            graph: new Spawn.TreeDependencyGraphView({
                model:this.model
            }).render()
        };
        this.$el.append(this.views.graph.$el);
        return this;
    },
    close:function(){
        this.$el.remove();
        return this;
    }
});
Spawn.JobDetailTabsView = Backbone.View.extend({
    tagName:"ul",
    className:"span12 nav nav-tabs",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.$el.css({
            "margin-left":"0px"
            ,"margin-bottom":"0px"
        });
    },
    render:function(){
        var template,model;
        if(window.location.hash.contains("clone")){
            template=_.template($("#cloneJobDetailTabsTemplate").html());
            model={
                job:this.model.toJSON()
                ,cloneId:this.model.cloneId
            };
        }
        else if(this.model.isNew()){
            template=_.template($("#newJobDetailTabsTemplate").html());
            model={
                job:this.model.toJSON()
            };
        }
        else{
            template=_.template($("#jobDetailTabsTemplate").html());
            model={
                job:this.model.toJSON()
            };
        }
        var html = template(model);
        this.$el.html(html);
        return this;
    }
});
Spawn.JobDetailView = Backbone.View.extend({
    className:"span12",
    events:{
        "change input[type='text'].trackable":"handleTrackableInputChange",
        "click input[type='checkbox'].trackable":"handleTrackableInputChange",
        "select input[type='checkbox'].trackable":"handleTrackableInputChange",
        "change select.trackable":"handleTrackableInputChange"
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
    },
    render:function(){
        this.views={
            jobActionView: new Spawn.JobActionView({
                model:this.model
            }).render(),
            jobTabsView: new Spawn.JobDetailTabsView({
                model:this.model
            }).render(),
            currentTab:undefined
        };
        this.views.jobActionView.$el.appendTo(this.$el);
        this.views.jobTabsView.$el.appendTo(this.$el);
        this.hasRendered=true;
        return this;
    },
    handleTrackableInputChange:function(event){
        this.model.trigger("user_edit");
    },
    close:function(){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.stopListening();
        this.$el.remove();
        return this;
    },
    renderSettings:function(){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#settingsTab").addClass("active");
        this.views.currentTab=new Spawn.JobSettingsView({
            model:this.model
        }).render();
        this.views.currentTab.$el.appendTo(this.$el);
        return this;
    },
    renderExpandedConfiguration:function(){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#expandConfTab").addClass("active");
        this.views.currentTab=new Spawn.JobExpandedConfView({
            model:this.model
        }).render();
        this.views.currentTab.$el.appendTo(this.$el);
        this.views.currentTab.refresh();
        return this;
    },
    renderConfiguration:function(){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#confTab").addClass("active");
        this.views.currentTab=new Spawn.JobConfView({
            model:this.model
        }).render();
        this.views.currentTab.$el.appendTo(this.$el);
        this.views.currentTab.refresh();
        return this;
    },
    renderTasks:function(){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#tasksTab").addClass("active");
        var taskCollection = this.model.getTaskCollection({
            collectionName:"taskCollection"
        });
        this.views.currentTab=new Spawn.JobTaskTableView({
            model:this.model,
            collection: taskCollection
        }).render();
        taskCollection.fetch({
            success:function(){
                taskCollection.search();
                taskCollection.filter();
            }
		});
        this.views.currentTab.$el.appendTo(this.$el);
        return this;
    },
    renderTaskDetail:function(taskNum){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#tasksTab").addClass("active");
        var taskCollection = this.model.getTaskCollection({
            collectionName:"taskCollection"
        });
        this.views.currentTab=new Spawn.SingleTaskView({
            model:this.model
            ,collection:taskCollection
            ,taskNum:taskNum
        }).render()
        taskCollection.fetch({
            success:function(){
                taskCollection.search();
                taskCollection.filter();
            }
		});
        this.views.currentTab.$el.appendTo(this.$el);
        return this;
    },
    renderAlerts:function(){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#alertsTab").addClass("active");
        this.views.currentTab=new Spawn.JobAlertsView({
            model:this.model
        }).render();
        this.views.currentTab.$el.appendTo(this.$el);
        return this;
    },
    renderHistory:function(history, config){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#historyTab").addClass("active");
        var jobHistory = new Spawn.JobHistoryView({
            model:this.model
        });
        if (history){
        	jobHistory.$el.data("history", history);
        }
        if (config){
        	jobHistory.$el.data("config", config);
        }
        this.views.currentTab=jobHistory.render();
        this.views.currentTab.$el.appendTo(this.$el);
        return this;
    },
    renderDependencies:function(){
        if(!_.isUndefined(this.views.currentTab)){
            this.views.currentTab.close();
        }
        this.views.jobTabsView.$el.find("li#dependenciesTab").addClass("active");
        var model = new Spawn.TreeGraphModel({
			jobId: this.model.id
		});
        this.views.currentTab=new Spawn.JobDepView({
            model: model
        });
        model.fetch();
        this.views.currentTab.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.HostDetailView= Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.views={};
        this.listenTo(this.model,"change",this.render);
    },
    render:function(){
        this.$el.empty();
        var runningCollection = new Spawn.JobCollection([],{
            dontAutoUpdate:true,
            collectionName:"hostJobCollection"
        });
        var self=this;
        _.each(this.model.get("running"),function(runJob){
            var jobModel = Spawn.jobCollection.get(runJob.jobUuid);
            runningCollection.add(jobModel);
        });
        this.views={
            runningTable: new Spawn.JobsTable({
                collection: runningCollection
                ,dontShowNewButton:true
                ,titleView: new Spawn.BreadcrumbView({
                    template:_.template($("#hostBreadcrumbsTemplate").html()),
                    model:this.model.toJSON()
                })
            }).render()
        };
        this.views.runningTable.$el.appendTo(this.$el);
        return this;
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        return this;
    },
    handleChange:function(event){
    }
});
Spawn.HostRightPanel = Backbone.View.extend({
    className:"span11 right-sidebar",
    events:{
        "click tr":"resizeElements"
    },
    initialize:function(options){
        _.bindAll(this);
        $(window).on("resize",this.resizeElements);
    },
    render:function(){
        this.views={
            tableView:undefined,
            detailView:undefined
        };
        this.resizeElements();
        return this;
    },
    resizeElements:function(event){
        var bodyHeight = this.$el.height();
        if(!_.isUndefined(this.views.tableView)){
            var tableView = this.views.tableView;
            var batchHeight = tableView.views.batchActions.$el.height();
            var headerHeight = tableView.views.header.$el.height();
            var tableBodyHeight = bodyHeight;
            this.views.tableView.views.bodyContainer.height(tableBodyHeight-batchHeight-headerHeight-10);
        }
    },
    closeTableView:function(){
        if(!_.isUndefined(this.views.tableView)){
            this.views.tableView.close();
            this.views.tableView=undefined;
        }
        return this;
    },
    closeDetailView:function(){
        if(!_.isUndefined(this.views.detailView)){
            this.views.detailView.close();
            this.views.detailView=undefined;
        }
        return this;
    },
    renderTableView:function(){
        this.closeDetailView();
        if(_.isUndefined(this.views.tableView)){
            this.views.tableView= new Spawn.HostsTable({
                collection:this.collection
            }).render();
            this.views.tableView.$el.appendTo(this.$el);
        }
        return this;
    },
    renderDetailView:function(){
        this.closeDetailView().closeTableView();
        if(_.isUndefined(this.views.tableView)){
            this.views.detailView=new Spawn.HostDetailView({
                model:this.model
            }).render();
            this.views.detailView.$el.appendTo(this.$el);
        }
        return this;
    }
});

Spawn.JobsRightPanel = Backbone.View.extend({
    className:"span11 right-sidebar",
    events:{
        "click tr":"resizeElements"
    },
    initialize:function(options){
        _.bindAll(this);
        this.taskCollection=options.taskCollection;
        $(window).on("resize",this.resizeElements);
    },
    render:function(){
        this.views={
            jobsTable:undefined,
            jobQuickView:undefined,
            jobDetailView:undefined,
            taskView:undefined
        };
        return this;
    },
    renderJobDetailView:function(){
        var isAlreadyShowing = !_.isUndefined(this.views.jobDetailView) && !_.isUndefined(this.views.jobDetailView.model) && !_.isEqual(this.views.jobDetailView.model.id,this.model.id);
        if(!isAlreadyShowing){
            this.closeJobDetailView();
        }
        if(!_.isUndefined(this.views.jobsTable)){
            this.closeJobsTableView();
        }
        this.closeQuickJobView().closeTaskView();
        if(!isAlreadyShowing){
            this.views.jobDetailView= new Spawn.JobDetailView({
                model:this.model
            }).render();
            this.views.jobDetailView.$el.appendTo(this.$el);
        }
        this.resizeElements();
        return this.views.jobDetailView;
    },
    renderJobsTable:function(){
        this.closeQuickJobView().closeTaskView();
        if(_.isUndefined(this.views.jobsTable)){
            this.views.jobsTable = new Spawn.JobsTable({
                collection: this.collection
            }).render();
            this.views.jobsTable.views.bodyContainer.css({
            });
        }
        if(!this.views.jobsTable.$el.is(":visible")){
            this.views.jobsTable.$el.appendTo(this.$el);
        }
        this.resizeElements();
        return this;
    },
    renderJobQuick:function(){
        this.closeTaskView();
        if(_.isUndefined(this.views.jobQuickView)){
            //Jobs QuickView
            this.views.jobQuickView = new Spawn.QuickJobView({
                collection: this.taskCollection,
                model:this.model
            }).render();
            this.views.jobQuickView.$el.appendTo(this.$el);
        }
        this.resizeElements();
        return this;
    },
    renderTaskView:function(taskNum){
        this.closeQuickJobView().closeTaskView();
        //Jobs QuickView
        this.views.taskView = new Spawn.QuickTaskView({
            collection:this.taskCollection,
            model:this.model,
            taskNum:taskNum
        }).render();
        this.views.taskView.$el.appendTo(this.$el);
        this.resizeElements();
        return this;
    },
    resizeElements:function(){
        var bodyHeight = this.$el.height();
        var quickViewSetup=false;
        if(!_.isUndefined(this.views.jobQuickView)){
           this.views.jobQuickView.views.taskTable.views.bodyContainer.height(bodyHeight*0.50 - 92);
           quickViewSetup=true;
        }
        if(!_.isUndefined(this.views.taskView)){
           this.views.taskView.$el.height(bodyHeight*0.50 - 122);
           this.views.taskView.views.singleTaskView.views.taskTable.views.bodyContainer.height(bodyHeight*0.50 - 92);
           if(!_.isUndefined(this.views.taskView.views.singleTaskView.views.logView)){
               this.views.taskView.views.singleTaskView.views.logView.$el.height(bodyHeight*0.50-60);
           }
           quickViewSetup=true;
        }
        if(!_.isUndefined(this.views.jobsTable)){
            var jobsTable = this.views.jobsTable;
            var batchHeight = jobsTable.views.batchActions.$el.height();
            var headerHeight = jobsTable.views.header.$el.height();
            var tableBodyHeight = (quickViewSetup?bodyHeight*0.50:bodyHeight);
            this.views.jobsTable.views.bodyContainer.height(tableBodyHeight-batchHeight-headerHeight-10);
        }
        if(!_.isUndefined(this.views.jobDetailView)){
            this.views.jobDetailView.$el.height(bodyHeight);
            if(!_.isUndefined(this.views.jobDetailView.views.currentTab)){
                var currentTab = this.views.jobDetailView.views.currentTab;
                var offset = currentTab.$el.offset();
                currentTab.$el.height(bodyHeight-65);
                if(_.has(currentTab,"refresh")){
                    currentTab.refresh();
                }
                if(_.has(currentTab,"views")){
                    if(_.has(currentTab.views,"taskTable")){
                        currentTab.views.taskTable.views.bodyContainer.height(bodyHeight-130);
                    }
                    if(_.has(currentTab.views,"logView")){
                        currentTab.views.logView.$el.height(bodyHeight-108);
                    }
                }
            }
        }
        return this;
    },
    closeQuickJobView:function(){
        if(!_.isUndefined(this.views.jobQuickView)){
            this.views.jobQuickView.$el.remove();
            this.views.jobQuickView=undefined;
        }
        return this;
    },
    closeTaskView:function(){
        if(!_.isUndefined(this.views.taskView)){
            this.views.taskView.close();
            this.views.taskView=undefined;
        }
        return this;
    },
    closeJobsTableView:function(){
        if(!_.isUndefined(this.views.jobsTable) && this.views.jobsTable.$el.is(":visible")){
            this.views.jobsTable.$el.detach();
        }
        return this;
    },
    closeJobDetailView:function(){
        if(!_.isUndefined(this.views.jobDetailView)){
            this.views.jobDetailView.close();
            this.views.jobDetailView=undefined;
        }
        return this;
    }
});
Spawn.RunningCountMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value=0;
        this.listenTo(this.collection,"reset",this.render);
        this.listenTo(this.collection,"change:state",this.render);
    },
    render:function(){
        var name = "running";
        var value=0;
        this.collection.forEach(function(job){
            value=value+job.get("running")-job.get("done");
        });
        this.value = value;
        var html = this.template({
            metricName:name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    }
});
Spawn.ErrorCountMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value=0;
        this.listenTo(this.collection,"reset",this.render);
        this.listenTo(this.collection,"change:state",this.render);
    },
    render:function(){
        var name = "error";
        var value=0;
        this.collection.forEach(function(job){
            value=value+job.get("errored");
        });
        this.value = value;
        var html = this.template({
            metricName:name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    }
});
Spawn.TaskCountMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value=0;
        this.listenTo(this.collection,"reset",this.render);
        this.listenTo(this.collection,"add",this.render);
        this.listenTo(this.collection,"remove",this.render);
    },
    render:function(){
        var name = "tasks";
        var value=0;
        this.collection.forEach(function(job){
            value=value+job.get("nodes");
        });
        this.value = value;
        var html = this.template({
            metricName:name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    }
});
Spawn.QueueCountMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value=Spawn.queueSize || "-";
        this.listenTo(Spawn.server,"task.queue.size",this.handleTaskQueueSizeChange);
    },
    render:function(){
        var name = "queued";
        var html = this.template({
            metricName:name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    },
    handleTaskQueueSizeChange:function(eventMessage){
        this.value=eventMessage.size;
        this.render();
    }
});
Spawn.JobCountMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value=0;
        this.listenTo(this.collection,"reset",this.render);
        this.listenTo(this.collection,"add",this.render);
        this.listenTo(this.collection,"remove",this.render);
    },
    render:function(){
        var name = "jobs";
        this.value = this.collection.length;
        var html = this.template({
            metricName:name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    }
});
Spawn.HostCountMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value="-";
        this.listenTo(this.collection,"reset",this.render);
        this.listenTo(this.collection,"add",this.render);
        this.listenTo(this.collection,"remove",this.render);
    },
    render:function(){
        var name = "hosts";
        if(!_.isUndefined(this.collection)){
            this.value = this.collection.length;
        }
        var html = this.template({
            metricName:name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    }
});
Spawn.TaskSlotsAvailableMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value="-";
        this.name="available";
        this.listenTo(this.collection,"reset",this.render);
        this.listenTo(this.collection,"add",this.handleHostAdd);
        this.listenTo(this.collection,"remove",this.handleHostRemove);
        this.listenTo(this.collection,"change:availableTaskSlots",this.handleHostChange);
    },
    render:function(){
        var value=0;
        this.collection.forEach(function(host){
            value=value+host.get("availableTaskSlots");
        });
        this.value = value;
        var html = this.template({
            metricName:this.name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    },
    handleHostAdd:function(host){
        this.value=this.value+host.get("availableTaskSlots");
        this.$el.html(this.template({
            metricName:this.name,
            metricValue:this.value
        }));
    },
    handleHostRemove:function(host){
        this.value=this.value-host.get("availableTaskSlots");
        this.$el.html(this.template({
            metricName:this.name,
            metricValue:this.value
        }));
    },
    handleHostChange:function(host){
        var previousValue = host.previous("availableTaskSlots");
        previousValue=(_.isUndefined(previousValue)?0:previousValue);
        this.value=this.value+(host.get("availableTaskSlots")-previousValue);
        this.$el.html(this.template({
            metricName:this.name,
            metricValue:this.value
        }));
    }
});
Spawn.DiskUsageMetricView = Backbone.View.extend({
    tagName:"span",
    className:"pull-right",
    events:{
    },
    template:_.template($("#metricBadgeTemplate").html()),
    initialize:function(){
        this.$el.css({
            "margin-left":"10px"
        });
        this.value="-";
        this.name="disk";
        this.listenTo(this.collection,"reset",this.render);
        this.listenTo(this.collection,"add",this.handleHostAdd);
        this.listenTo(this.collection,"remove",this.handleHostRemove);
        this.listenTo(this.collection,"change:diskUsed",this.handleDiskChange);
        this.listenTo(this.collection,"change:diskMax",this.handleDiskChange);
    },
    render:function(){
        var diskMax=0,diskUsed=0;
        this.collection.forEach(function(host){
            diskUsed=diskUsed+host.get("used").disk;
            diskMax=diskMax+host.get("max").disk;
        });
        this.value = (diskMax>0?Math.round( (diskUsed/diskMax)*100)/100:"-");
        var html = this.template({
            metricName:this.name,
            metricValue:this.value
        });
        this.$el.html(html);
        return this;
    },
    handleHostAdd:function(host){
        this.value=this.value+(host.get("diskMax")>0?host.get("diskUsed")/host.get("diskMax"):0);
        var html = this.template({
            metricName:this.name,
            metricValue:this.value
        });
        this.$el.html(html);
    },
    handleHostRemove:function(host){
        this.value=this.value-(host.get("diskMax")>0?host.get("diskUsed")/host.get("diskMax"):0);
        var html = this.template({
            metricName:this.name,
            metricValue:this.value
        });
        this.$el.html(html);
    },
    handleHostChange:function(host){
        var previousValue = this.value-(host.previous("diskMax")>0?host.previous("diskUsed")/host.previous("diskMax"):0);
        var value=(host.get("diskMax")>0?host.get("diskUsed")/host.get("diskMax"):0);
        this.value=this.value+(value-previousValue);
        var html = this.template({
            metricName:this.name,
            metricValue:this.value
        });
        this.$el.html(html);
    }
});
Spawn.MetricBadgesView = Backbone.View.extend({
    tagName:"div",
    className:"span12",
    events:{
    },
    initialize:function(options){
        this.$el.css({
            "font-size":"12px"
        });
        this.hostCollection = Spawn.hostCollection;
        this.collection=Spawn.jobCollection;
    },
    render:function(){
        this.views={
            jobCountMetric: new Spawn.JobCountMetricView({
                collection:this.collection
            }).render()
            ,hostCountMetric: new Spawn.HostCountMetricView({
                collection:this.hostCollection
            }).render()
            ,taskCountMetric: new Spawn.TaskCountMetricView({
                collection:this.collection
            }).render()
            ,runningCountMetric: new Spawn.RunningCountMetricView({
                collection:this.collection
            }).render()
            ,errorCountMetric: new Spawn.ErrorCountMetricView({
                collection:this.collection
            }).render()
            ,queueCountMetric: new Spawn.QueueCountMetricView({
            }).render()
            ,availableCountMetric: new Spawn.TaskSlotsAvailableMetricView({
                collection:this.hostCollection
            }).render()
            ,diskUsageMetric: new Spawn.DiskUsageMetricView({
                collection:this.hostCollection
            }).render()
        };
        this.views.diskUsageMetric.$el.appendTo(this.$el);
        this.views.availableCountMetric.$el.appendTo(this.$el);
        this.views.runningCountMetric.$el.appendTo(this.$el);
        this.views.errorCountMetric.$el.appendTo(this.$el);
        this.views.queueCountMetric.$el.appendTo(this.$el);
        this.views.taskCountMetric.$el.appendTo(this.$el);
        this.views.hostCountMetric.$el.appendTo(this.$el);
        this.views.jobCountMetric.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.JobsView = Backbone.View.extend({
    el:"div#jobs",
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.hasRendered=false;
        this.selected={};
        this.views={};
        this.rightView=null;
        if(_.has(options,"hostCollection")){
            this.hostCollection=options.hostCollection;
        }
    },
    handleFilterClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        //TODO: check if model is dirty and prompt user for confirmation, do they want to navigate away with unsaved changes?
        if(!_.isUndefined(this.views.jobRightPanel.views.jobDetailView)){
            Spawn.router.navigate("#jobs",{trigger:true});
        }
        return this;
    },
    render:function(){
        this.$el=$(this.el);
        this.show();
        if(_.isUndefined(this.views.metricBadges)){
            this.views.metricBadges = new Spawn.MetricBadgesView({
                collection:this.collection
                ,hostCollection:this.hostCollection
            }).render();
            this.views.metricBadges.$el.appendTo(this.$el);
        }
        if(_.isUndefined(this.views.sideBarPanel)){
            this.views.sideBarPanel = new Spawn.JobsSideFilter({
                collection:this.collection
            }).render();
            this.views.sideBarPanel.$el.appendTo(this.$el);
        }
        if(_.isUndefined(this.views.jobRightPanel)){
            this.views.jobRightPanel = new Spawn.JobsRightPanel({
                collection:this.collection,
                model:this.model
            }).render();
            this.views.jobRightPanel.$el.appendTo(this.$el);
        }
        return this;
    },
    renderJobsTable:function(){
        this.views.jobRightPanel.closeJobDetailView();
        this.views.jobRightPanel.renderJobsTable();
        this.$el.show();
        return this;
    },
    renderQuickJobView:function(){
        var self=this;
        this.views.jobRightPanel.model=this.model;
        this.views.jobRightPanel.taskCollection=this.model.getTaskCollection({
            collectionName:"taskCollection"
        });
        this.views.jobRightPanel.renderJobQuick();
        this.views.jobRightPanel.taskCollection.fetch({
            success:function(){
                self.views.jobRightPanel.taskCollection.search();
                self.views.jobRightPanel.taskCollection.filter();
            }
		});
        return this;
    },
    renderTaskView:function(taskNum){
        var self=this;
        this.views.jobRightPanel.model=this.model;
        this.views.jobRightPanel.taskCollection=this.model.getTaskCollection();
        this.views.jobRightPanel.renderTaskView(taskNum);
        this.views.jobRightPanel.taskCollection.fetch({
            success:function(){
                self.views.jobRightPanel.taskCollection.search();
                self.views.jobRightPanel.taskCollection.filter();
            }
		});
    },
    renderJobDetailView:function(){
        this.views.jobRightPanel.model=this.model;
        var detailView = this.views.jobRightPanel.renderJobDetailView();
        return detailView;
    },
    closeQuickView:function(){
        if(!_.isUndefined(this.views.jobRightPanel)){
            this.views.jobRightPanel.closeQuickJobView().closeTaskView();
        }
        return this;
    },
    closeJobsTableView:function(){
        if(!_.isUndefined(this.views.jobRightPanel)){
            this.views.jobRightPanel.closeJobsTableView().closeQuickJobView().closeTaskView();
        }
        return this;
    },
    closeJobDetailView:function(){
        this.views.jobRightPanel.closeJobDetailView();
        return this;
    },
    show:function(){
        this.$el.show();
    },
    hide:function(){
        this.$el.hide();
    }
});
Spawn.GitModel = Backbone.Model.extend({
    url:"/update/git.properties",
    defaults:{
        branch:"",
        buildTime:"",
        buildUserEmail:"",
        buildUserName:"",
        commitId:"",
        commitIdAbbrev:"",
        commitIdDescribe:"",
        commitMessageFull:"",
        commitTime:"",
        commitUserEmail:"",
        commitUserName:""
    }
});
//util
function convertToDFH(bytes){
	if(typeof bytes=="number" && bytes>=0){
		var units = ["B","K","M","G","T"];
        var shorterSize = bytes;
        for(var u=0;u<units.length && Math.floor(shorterSize/10)>10;u++){
            shorterSize=(shorterSize/1024).toFixed(1);
        }
        return shorterSize+" "+units[u];
	}
	else{
		return "-";
	}
}

function shorten(word, len){
    return (word.length>len? word.substr(0,len)+"...": word);
}

function shortenNumber(num){
	if(num && num>0){
        var units = ["","K","M"];
        var shorterSize = num.toFixed(2);
        for(var u=0;u<units.length && shorterSize>1000;u++){
            shorterSize=(shorterSize/1000).toFixed(2);
        }
        return shorterSize+" "+units[u];
    }
    else{
        return "-";
    }
}

function convertToDateTimeText(time,format){
    format= format || "d-MMM-yy h:mm tt";
    if(time)
		return new Date(time).toString(format);
    else
		return "-";
}

function getStatusLabel(status,disabled){
    var label = $("<div class='task-status label'>"+status+"</div>");
    if(status=="ERROR"){
		label.addClass("label-important");
    }
    else if(status=="RUNNING" || status=="BUSY"){
		label.addClass("label-success");
    }
    else if(status=="QUEUED" || status=="REPLICATE" || status=="SCHEDULED"  || status=="REBALANCE"){
		label.addClass("label-info");
    }
    else if(status=="DEGRADED" || status=="UNKNOWN"){
        label.addClass("label-inverse");
    }
    else{
    }
    if(disabled)
        label.append(" (D)");
    return label;
}
//al's function from spawn1
Spawn.descriptionForErrorCode= function(code){
	if (code > 0) {
		return "job error: " + code;
	}
	else if(code<0){
		switch(code) {
		case -100:
			return "backup failed";
		case -101:
			return "replicate failed";
		case -102:
			return "revert failed";
		case -103:
			return "swap failed";
		case -104:
			return "failed host";
		case -105:
			return "kick failed";
		case -106:
			return "dir error";
		case -107:
			return "script exec error";
		default:
			return "unknown";
		}
	}
    else{
        return "-";
    }
};
Spawn.getTaskStatusText= function(task){
    var text="";
	if (task.errorCode > 0) {
		text+= "job error: " + task.errorCode;
	}
	else if(task.errorCode<0){
		switch(task.errorCode) {
            case -100:
                text+= "backup failed";
            case -101:
                text+= "replicate failed";
            case -102:
                text+= "revert failed";
            case -103:
                text+= "swap failed";
            case -104:
                text+= "failed host";
            case -105:
                text+= "kick failed";
            case -106:
                text+= "dir error";
            case -107:
                text+= "script exec error";
            default:
                text+= "unknown";
		}
	}
    else{
        if(task.wasStopped){
            text+= "stopped";
        }
        else{
            text+= "-";
        }
    }
    return "<i>"+text+"</i>";
};
Spawn.JobStateEnum = {
	idle: 0,
	scheduled: 1,
	running: 2,
	degraded: 3,
	unknown:4,
	error:5,
	rebalance:6
};
Spawn.JobStates = [
	{
        name:"IDLE",
        shortName:"IDL",
        class:"",
        number:function(job){
            return job.nodes;
        }
    },
	{
        name:"SCHEDL",
        shortName:"SDL",
        class:"label-info",
        number:function(job){
            return job.nodes-job.done;
        }
    },
	{
        name:"RUNNING",
        shortName:"RUN",
        class:"label-success",
        number:function(job){
            return job.running-job.done;
        }
    },
	{name:"DEGRD",shortName:"DEG",class:"label-inverse"},
	{name:"UNKNW",shortName:"UNK",class:"label-inverse"},
	{
        name:"ERROR",
        shortName:"ERR",
        class:"label-important",
        number:function(job){
            return job.errored;
        }
    },
	{name:"REBALANCE",shortName:"REB",class:"label-info"}
];
Spawn.TaskStateEnum = {
   idle:0,
   busy:1,
   error:2,
   allocated:3,
   backup:4,
   replicate:5,
   unknown:6,
   rebalance:7,
   revert:8,
   diskFull:9,
   swapping:10,
   queued:11
};
Spawn.TaskStates= [
	{name:"IDLE",class:""},
	{name:"BUSY",class:"label-success"},
	{name:"ERROR",class:"label-important"},
	{name:"ALLOCATED",class:"label-info"},
    {name:"BACKUP",class:"label-success"},
    {name:"REPLICATE",class:"label-success"},
	{name:"UNKNOWN",class:"label-inverse"},
    {name:"REBALANCE",class:"label-info"},
    {name:"REVERT",class:"label-info"},
    {name:"DISK FULL",class:"label-important"},
    {name:"SWAPPING",class:"label-info"},
    {name:"QUEUED",class:"label-info"}
];
var CheckDirsModalView =Backbone.View.extend({
    events: {
        'click a': 'handleSort',
        'click button:not(.close)': 'handleTaskFix'
    },
    initialize:function(options){
        this.options=options;
        this.downIcon=$("<i>&darr;</i>");
        this.upIcon=$("<i>&uarr;</i>");
        this.transitions={
            "asc":"desc",
            "desc":"none",
            "none":"asc"
        };
    },
    handleTaskFix: function(event){
        var button = $(event.currentTarget);
        if(!button.is(".disable")){
            var job = Spawn.jobCollection.get(button.data("job"));
            job.fixDirs(button.data("node"));
        }
    },
    handleSort: function(event) {
        event.preventDefault();
        var orderby;
        var column=$(event.currentTarget);
        if(_.isUndefined(column.data("orderby"))){
            orderby="asc";
        }
        else{
            orderby=this.transitions[column.data("orderby")];
        }
        var index = $(event.currentTarget).data("sortby");
        var table = this.$el.find("div.table-body-full-height table.table tbody");
        var rows = table.children("tr.row");
        var detachedList = [];
        this.detachIcons();
        var sortedList;
        if(_.isEqual(orderby,"none")){
            sortedList=_.shuffle(rows);
        }
        else{
            sortedList=_.sortBy(rows,function(row){
                var child = $(row).children().eq(index);
                var text = $.trim($(child).text());
                return text;
            });
            if(_.isEqual(orderby,"asc")){
                column.parent().append(this.downIcon);
            }
            else if(_.isEqual(orderby,"desc")){
                column.parent().append(this.upIcon);
            }
        }
        _.each(sortedList,function(row){
            var detached = $(row).detach();
            if(_.isEqual(orderby,"asc")){
                table.append(detached);
            }
            else{
                table.prepend(detached);
            }
        });
        column.data("orderby",orderby);
    },
    template:_.template($("#checkDirsModalTemplate").html()),
    render:function(){
        //this.$el.attr("class","");
        this.$el.attr("tabindex","-1");
        this.$el.attr("role","dialog");
        this.$el.attr("aria-labelledby","myModalLabel");
        this.$el.attr("aria-hidden","true");
        this.$el.html(this.template({
            jobId:this.options.job.id,
            jobTitle:this.options.job.description,
            matches:this.options.matches
        }));
        if(!this.$el.is(":visible")){
            this.$el.modal("show");
        }
        return this;
    },
    className:"modal hide fade bigModal",
    detachIcons:function(){
        this.downIcon.detach();
        this.upIcon.detach();
    }
});
Spawn.getJobStatusVerb = function(job){
    var status = "-";
    if(job.disabled){
        status="disabled";
    }
    else if(_.isEqual(job.running,job.done) && (job.running<job.nodes)){
        status="blocked";
    }
    else if(_.isEqual(job.state,0) && job.wasStopped){
        status="stopped";
    }
    return status;
};
Spawn.deparam = function(paramString){
    var result = {};
    if( ! paramString){
        return result;
    }
    $.each(paramString.split('&'), function(index, value){
        if(value){
            var param = value.split('=');
            result[param[0]] = param[1];
        }
    });
    return result;
};
Spawn.Router = Backbone.Router.extend({
    initialize:function(options){
        this.params={};
    },
    routes: {
        "":"showIndex",
        "jobs":"showJobsTable",
        "jobs/new/conf":"showNewJobConf",
        "jobs/new/expandConf":"showExpNewJobConf",
        "jobs/new/settings":"showNewJobSettings",
        "jobs/new/alerts":"showNewJobAlerts",
        "jobs/:jobId/clone/conf":"showNewJobConf",
        "jobs/:jobId/clone/expandConf":"showExpNewJobConf",
        "jobs/:jobId/clone/settings":"showNewJobSettings",
        "jobs/:jobId/clone/alerts":"showNewJobAlerts",
        "jobs/:jobId/quick":"showQuickJob",
        "jobs/:jobId/quick/:taskNum":"showQuickTask",
        "jobs/:jobId/conf":"showJobConfPanel",
        "jobs/:jobId/expandConf":"showExpJobConfPanel",
        "jobs/:jobId/settings":"showJobSettings",
        "jobs/:jobId/history":"showJobHistory",
        "jobs/:jobId/tasks":"showJobTaskPanel",
        "jobs/:jobId/tasks/:taskNum":"showJobTaskDetail",
        "jobs/:jobId/alerts":"showJobAlerts",
        "jobs/:jobId/dependencies":"showJobDepPanel",
        "jobs/:jobId":"showJobDetail",
        "macros":"showMacroTable",
        "macros/new":"showNewMacro",
        "macros/:name":"showMacroDetail",
        "commands":"showCommandTable",
        "commands/new":"showNewCommand",
        "commands/:name":"showCommandDetail",
        "aliases":"showAliasTable",
        "aliases/new":"showNewAlias",
        "aliases/:name":"showAliasDetail",
        "hosts":"showHostTable",
        "hosts/:hostId":"showHostDetail",
        "hosts/:hostId/ganglia":"showHostGangliaGraphs",
        "settings":"showSettings",
        "balanceParams":"showBalanceParams"
    },
    getParams:function(){
        return this.params;
    },
    setParams:function(params){
        _.extend(this.params,params);
        this.setParamHash($.param(this.params));
    },
    clearParams:function(){
        this.clearParamHash();
        this.params={};
    },
    clearParamHash:function(){
        var url = window.location.hash;
        var paramIndex = url.indexOf("?");
        if(paramIndex>-1){
            var paramHash = url.substring(paramIndex);
            this.navigate(paramHash,{trigger:false});
        }
    },
    setParamHash:function(hash){
        this.clearParamHash();
        this.navigate(window.location.hash+"?"+hash,{trigger:false});
    }
});

(function($) {
    $.fn.hasScrollBar = function() {
        return this.get(0).scrollHeight > this.height();
    }
	$.fn.appendEach = function( arrayOfWrappers ){

	// Map the array of jQuery objects to an array of
	// raw DOM nodes.
	var rawArray = jQuery.map(
	arrayOfWrappers,
	function( value, index ){

	// Return the unwrapped version. This will return
	// the underlying DOM nodes contained within each
	// jQuery value.
	return( value.get() );

	}
	);

	// Add the raw DOM array to the current collection.
	this.append( rawArray );

	// Return this reference to maintain method chaining.
	return( this );

	};
})(jQuery);
Spawn.SideFilterLayoutView = Backbone.View.extend({
    className:"span12",
    initialize:function(options){
        _.bindAll(this);
        this.views={};
    },
    events:{
        "click div.sidebar li a":"handleFilterClick"
    },
    render:function(){
        if(_.isUndefined(this.views.metricBadges)){
            this.views.metricBadges = new Spawn.MetricBadgesView({
                collection:this.collection
            }).render();
            this.views.metricBadges.$el.appendTo(this.$el);
        }
        if(_.isUndefined(this.views.sideBarPanel)){
            this.views.sideBarPanel = new this.sideFilterClass({
                collection:this.collection
            }).render();
            this.views.sideBarPanel.$el.appendTo(this.$el);
        }
        return this;
    },
    handleFilterClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        return this;
    },
    hide:function(){
        this.$el.hide();
        return this;
    },
    show:function(){
        this.$el.show();
        return this;
    },
    renderMainView:function(){
        if(_.isUndefined(this.views.mainView)){
            this.views.mainView = new this.mainViewClass({
                collection:this.collection
            }).render();
            this.views.mainView.$el.appendTo(this.$el);
        }
        return this.views.mainView;
    }
});
Spawn.AliasBatchActions = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "background":"#EEE",
            "padding-top":"4px"
        });
        this.collection=Spawn.aliasCollection;
    },
    render:function(){
        this.views={
            selectButton:new Spawn.SelectBatchButton({
                collection:this.collection,
                css:{
                    "width":"64px",
                    "margin-left":"10px"
                }
            }).render()
            ,deleteButton:new Spawn.DeleteBatchButton({
                collection:this.collection,
                css:{
                    "width":"64px",
                    "margin-left":"30px",
                    "color":"red"
                }
            }).render()
            ,searchbox: new Spawn.TypeaheadBox({
                collection:this.collection,
                className:"search-query pull-right",
                keys:["name"],
                placeholder:"Search Alias.."
            }).render()
            ,createButton: new Spawn.CreateButton({
                //title:"Create Command"
                title:"Create"
                ,createUrl:"#aliases/new"
            }).render()
        };
        this.views.createButton.$el.appendTo(this.$el);
        this.views.selectButton.$el.appendTo(this.$el);
        this.views.deleteButton.$el.appendTo(this.$el);
        this.views.searchbox.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.CommandBatchActions = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "background":"#EEE",
            "padding-top":"4px"
        });
        this.collection=Spawn.commandCollection;
    },
    render:function(){
        this.views={
            selectButton:new Spawn.SelectBatchButton({
                collection:this.collection,
                css:{
                    "width":"64px",
                    "margin-left":"10px"
                }
            }).render()
            ,deleteButton:new Spawn.DeleteBatchButton({
                collection:this.collection,
                css:{
                    "width":"64px",
                    "margin-left":"30px",
                    "color":"red"
                }
            }).render()
            ,searchbox: new Spawn.TypeaheadBox({
                collection:this.collection,
                className:"search-query pull-right",
                keys:["name","owner"],
                placeholder:"Search Commands.."
            }).render()
            ,createButton: new Spawn.CreateButton({
                //title:"Create Command"
                title:"Create"
                ,createUrl:"#commands/new"
            }).render()
        };
        this.views.createButton.$el.appendTo(this.$el);
        this.views.selectButton.$el.appendTo(this.$el);
        this.views.deleteButton.$el.appendTo(this.$el);
        this.views.searchbox.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.AliasTableView = Spawn.SelectableTableView.extend({
    batchActionClass: Spawn.AliasBatchActions
    ,headerTemplate:"#aliasTableHeaderTemplate"
    ,tableRowTemplate:"#aliasTableRowTemplate"
});
Spawn.CommandTableView = Spawn.SelectableTableView.extend({
    batchActionClass: Spawn.CommandBatchActions
    ,headerTemplate:"#commandTableHeaderTemplate"
    ,tableRowTemplate:"#commandTableRowTemplate"
});
Spawn.MacroBatchActions = Backbone.View.extend({
    className:"span12",
    events:{
    },
    initialize:function(options){
        _.bindAll(this);
        this.selected={};
        this.views={};
        this.$el.css({
            "background":"#EEE",
            "padding-top":"4px"
        });
        this.collection=Spawn.macroCollection;
    },
    render:function(){
        this.views={
            selectButton:new Spawn.SelectBatchButton({
                collection:this.collection,
                css:{
                    "width":"64px",
                    "margin-left":"10px"
                }
            }).render()
            ,deleteButton:new Spawn.DeleteBatchButton({
                collection:this.collection,
                css:{
                    "width":"64px",
                    "margin-left":"30px",
                    "color":"red"
                }
            }).render()
            ,searchbox: new Spawn.TypeaheadBox({
                collection:this.collection,
                className:"search-query pull-right",
                keys:["name","description","owner"],
                placeholder:"Search Macros.."
            }).render()
            ,createButton: new Spawn.CreateButton({
                title:"Create"
                ,createUrl:"#macros/new"
            }).render()
        };
        this.views.createButton.$el.appendTo(this.$el);
        this.views.selectButton.$el.appendTo(this.$el);
        this.views.deleteButton.$el.appendTo(this.$el);
        this.views.searchbox.$el.appendTo(this.$el);
        return this;
    }
});
Spawn.MainView = Backbone.View.extend({
    className:"span11 right-sidebar",
    events:{
        "click tr":"resizeElements"
    },
    initialize:function(options){
        _.bindAll(this);
        $(window).on("resize",this.resizeElements);
        this.hasRendered=false;
    },
    render:function(){
        if(!this.hasRendered){
            this.views={
                tableView:undefined,
                detailView:undefined
            };
        }
        this.$el.show();
        this.hasRendered=true;
        return this;
    },
    resizeElements:function(event){
        var bodyHeight = this.$el.height();
        if(!_.isUndefined(this.views.tableView)){
            var tableView = this.views.tableView;
            var batchHeight = tableView.views.batchActions.$el.height();
            var headerHeight = tableView.views.header.$el.height();
            var tableBodyHeight = bodyHeight;
            this.views.tableView.views.bodyContainer.height(tableBodyHeight-batchHeight-headerHeight-10);
        }
        if(!_.isUndefined(this.views.detailView)){
            this.views.detailView.$el.height(bodyHeight-30);
            if(_.has(this.views.detailView,"refresh")){
                this.views.detailView.refresh();
            }
        }
    },
    closeTableView:function(){
        if(!_.isUndefined(this.views.tableView)){
            this.views.tableView.close();
            this.views.tableView=undefined;
        }
        return this;
    },
    closeDetailView:function(){
        if(!_.isUndefined(this.views.detailView)){
            this.views.detailView.close();
            this.views.detailView=undefined;
        }
        return this;
    },
    renderTableView:function(){
        this.closeDetailView();
        if(_.isUndefined(this.views.tableView)){
            this.views.tableView= new this.tableViewClass({
                collection:this.collection
            }).render();
            this.views.tableView.$el.appendTo(this.$el);
        }
        this.resizeElements();
        return this;
    },
    renderDetailView:function(){
        this.closeTableView();
        if(_.isUndefined(this.views.detailView)){
            this.views.detailView=new this.detailViewClass({
                model:this.model
            }).render();
            this.views.detailView.$el.appendTo(this.$el);
        }
        this.resizeElements();
        if(_.has(this.views.detailView,"refresh")){
            this.views.detailView.refresh();
        }
        return this;
    }
});
Spawn.AceEditorView = Backbone.View.extend({
    events:{
        "change":"handleEditorChange"
    },
    initialize:function(options){
        _.bindAll(this);
        this.keyName=options.keyName;
        this.readOnly=(options.readOnly?options.readOnly:false);
        this.hasRendered=false;
        this.hasReset=false;
        this.listenTo(this.model,"change:"+options.keyName,this.render);
        //this.listenTo(this.model,"reset",this.handleReset);
        this.model.bind("reset",this.handleReset);
    },
    handleEditorChange:function(event){
        this.model.set(this.keyName, this.views.editor.session.getValue(),{silent:true});
        if(this.hasReset){
            this.model.trigger("user_edit");
        }
    },
    handleReset:function(event){
        this.views.editor.session.setValue(this.model.get(this.keyName));
        this.hasReset=true;
    },
    render:function(){
        this.$el.css({
            "width":"100%"
            ,"height":"90%"
        });
        this.views={
            editor: ace.edit(this.el)
        };
        this.views.editor.setTheme("ace/theme/xcode");
        if(this.readOnly){
            this.views.editor.setReadOnly(this.readOnly);
        }
        this.views.editor.setShowPrintMargin(false);
        this.views.editor.getSession().setMode("ace/mode/javascript");
        this.views.editor.getSession().on("change",this.handleEditorChange);
        this.views.editor.session.setUseWorker(false);
        this.views.editor.session.setValue(this.model.get(this.keyName));
        this.hasRendered=true;
        this.views.editor.commands.addCommands([{
			 name: "unfind",
			 bindKey: {
				 win: "Ctrl-F",
				 mac: "Command-F"
			 },
			 exec: function(editor, line) {
				 return false;
			 },
			 readOnly: true
		 }]);
        return this;
    },
    handleChange:function(event){
        this.views.editor.session.setValue(this.model.get(this.keyName));
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        return this;
    }
});
Spawn.CommandDetailView = Backbone.View.extend({
    events:{
        "click #saveButton":"handleSave"
        ,"click #deleteButton":"handleDelete"
        ,"change #commandInput":"handleCommandInputChange"
        ,"blur #commandInput":"handleCommandInputChange"
        ,"change #nameInput":"handleNameInputChange"
        ,"blur #nameInput":"handleNameInputChange"
    },
    template:_.template($("#commandDetailTemplate").html()),
    render:function(){
        var html = this.template(this.model.toJSON());
        this.$el.html(html);
        return this;
    },
    handleCommandInputChange:function(event){
        var textArea = $(event.currentTarget);
        var commands = _.compact(textArea.val().split("\n"));
        this.model.set("command",commands);
        console.log(this.model.get("command"));
        return true;
    },
    handleNameInputChange:function(event){
        var nameInput = $(event.currentTarget);
        this.model.set("name",nameInput.val());
        console.log(this.model.get("name"));
        return true;
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        this.model=undefined;
        return this;
    },
    handleSave:function(event){
        event.preventDefault();
        this.model.save();
    },
    handleDelete:function(event){
        event.preventDefault();
        this.model.delete();
    }
});
Spawn.CommandMainView = Spawn.MainView.extend({
    detailViewClass:Spawn.CommandDetailView
    ,tableViewClass:Spawn.CommandTableView
});
Spawn.AliasDetailView = Backbone.View.extend({
    events:{
        "click #saveButton":"handleSave"
        ,"click #deleteButton":"handleDelete"
        ,"change #jobInput":"handleJobInputChange"
        ,"blur #jobInput":"handleJobInputChange"
        ,"change #nameInput":"handleNameInputChange"
        ,"blur #nameInput":"handleNameInputChange"
    },
    template:_.template($("#aliasDetailTemplate").html()),
    render:function(){
        var html = this.template(this.model.toJSON());
        this.$el.html(html);
        return this;
    },
    close:function(){
        this.stopListening();
        this.$el.remove();
        this.model=undefined;
        return this;
    },
    handleSave:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.save();
    },
    handleDelete:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        this.model.delete();
    },
    refresh:function(){
        return this;
    },
    handleJobInputChange:function(event){
        var textArea = $(event.currentTarget);
        var jobs = _.compact(textArea.val().split("\n"));
        this.model.set("jobs",jobs);
        console.log(this.model.get("jobs"));
        return true;
    },
    handleNameInputChange:function(event){
        var nameInput = $(event.currentTarget);
        this.model.set("name",nameInput.val());
        console.log(this.model.get("name"));
        return true;
    }
});
Spawn.MacroDetailView = Backbone.View.extend({
    events:{
        "click #saveButton":"handleSave"
        ,"click #deleteButton":"handleDelete"
    },
    template:_.template($("#macroDetailTemplate").html()),
    render:function(){
        var html = this.template(this.model.toJSON());
        this.$el.html(html);
        this.views={
            editor: new Spawn.AceEditorView({
                model:this.model,
                keyName:"macro"
            }).render()
        };
        this.views.editor.$el.appendTo(this.$el);
        return this;
    },
    close:function(){
        this.stopListening();
        this.views.editor.close();
        this.$el.remove();
        this.model=undefined;
        return this;
    },
    handleSave:function(event){
        event.preventDefault();
        this.model.set({
            name:this.$el.find("input#macroName").val()
            ,description: this.$el.find("input#macroDesc").val()
        });
        this.model.save();
    },
    handleDelete:function(event){
        event.preventDefault();
        this.model.delete();
    },
    refresh:function(){
        this.views.editor.views.editor.resize();
        return this;
    }
});
Spawn.MacroTableView = Spawn.SelectableTableView.extend({
    batchActionClass: Spawn.MacroBatchActions
    ,headerTemplate:"#macroTableHeaderTemplate"
    ,tableRowTemplate:"#macroTableRowTemplate"
});
Spawn.AliasMainView = Spawn.MainView.extend({
    detailViewClass:Spawn.AliasDetailView
    ,tableViewClass:Spawn.AliasTableView
});
Spawn.MacroMainView = Spawn.MainView.extend({
    detailViewClass:Spawn.MacroDetailView
    ,tableViewClass:Spawn.MacroTableView
});
Spawn.MacrosView = Spawn.SideFilterLayoutView.extend({
    el:"div#macros"
    ,sideFilterClass:Spawn.MacroFilterView
    ,mainViewClass: Spawn.MacroMainView
    ,render:function(){
        this.$el=$(this.el);
        Spawn.SideFilterLayoutView.prototype.render.call(this);
        this.$el.show();
        return this;
    }
});
Spawn.AliasFilterView = Spawn.SideFilterView.extend({
    initialize:function(opts){
        _.bindAll(this);
        this.views={};
        this.$el.css({
            "margin-left":"0px"
        });
        this.selected={};
        this.views={};
        this.criteria={};
        this.collection=Spawn.aliasCollection;
        if(!_.isUndefined(this.collection.collectionName) && !_.isUndefined($.cookie(this.collectionName))){
			var cookie = $.cookie(this.collection.collectionName) || {};
			this.criteria = (!_.isUndefined(cookie.criteria)?cookie.criteria:{});
        }
        else{
			this.criteria= {};
        }
        this.listenTo(this.collection,"change",this.handleCollectionChange);
    },
    render:function(){
        //Sidebar: All
        this.views.sidebarAll = new Spawn.SidebarView({
            template: _.template($("#aliasSidebarAllTemplate").html()),
            key:"*",
            collection:this.collection
        }).render();
        this.$el.append(this.views.sidebarAll.$el);
        return this;
    }
});
Spawn.AliasesView = Spawn.SideFilterLayoutView.extend({
    el:"div#aliases"
    ,sideFilterClass:Spawn.AliasFilterView
    ,mainViewClass: Spawn.AliasMainView
    ,render:function(){
        this.$el=$(this.el);
        Spawn.SideFilterLayoutView.prototype.render.call(this);
        this.$el.show();
        return this;
    }
});
Spawn.CommandFilterView = Spawn.SideFilterView.extend({
    initialize:function(){
        _.bindAll(this);
        this.views={};
        this.$el.css({
            "margin-left":"0px"
        });
        this.selected={};
        this.views={};
        this.criteria={};
        this.collection=Spawn.commandCollection;
        if(!_.isUndefined(this.collection.collectionName) && !_.isUndefined($.cookie(this.collectionName))){
			var cookie = $.cookie(this.collection.collectionName);
			this.criteria = cookie.criteria || {};
        }
        else{
			this.criteria= {};
        }
        this.listenTo(this.collection,"change",this.handleCollectionChange);
    },
    render:function(){
        //Sidebar: All
        this.views.sidebarAll = new Spawn.SidebarView({
            template: _.template($("#commandSidebarAllTemplate").html()),
            key:"*",
            collection:this.collection
        }).render();
        this.$el.append(this.views.sidebarAll.$el);
        return this;
    }
});
Spawn.HostsView = Backbone.View.extend({
    el:"div#hosts",
    className:"span12",
    initialize:function(options){
        _.bindAll(this);
        this.views={};
        if(_.has(options,"jobCollection")){
            this.jobCollection=options.jobCollection;
        }
    },
    events:{
        "click div.hosts-sidebar li a":"handleFilterClick"
    },
    render:function(){
        this.$el=$(this.el);
        if(_.isUndefined(this.views.metricBadges)){
            this.views.metricBadges = new Spawn.MetricBadgesView({
                collection:this.jobCollection
                ,hostCollection:this.collection
            }).render();
            this.views.metricBadges.$el.appendTo(this.$el);
        }
        if(_.isUndefined(this.views.sideBarPanel)){
            this.views.sideBarPanel = new Spawn.HostSideFilter({
                collection:this.collection
            }).render();
            this.views.sideBarPanel.$el.appendTo(this.$el);
        }
        if(_.isUndefined(this.views.hostRightPanel)){
            this.views.hostRightPanel = new Spawn.HostRightPanel({
                collection:this.collection,
                model:this.model
            }).render();
            this.views.hostRightPanel.$el.appendTo(this.$el);
        }
        this.show();
        return this;
    },
    renderHostTable:function(){
        this.views.hostRightPanel.closeDetailView();
        this.views.hostRightPanel.renderTableView();
        return this;
    },
    renderHostDetail:function(){
        this.views.hostRightPanel.closeTableView();
        this.views.hostRightPanel.model=this.model;
        this.views.hostRightPanel.renderDetailView();
        return this;
    },
    show:function(){
        this.$el.show();
        return this;
    },
    hide:function(){
        this.$el.hide();
        return this;
    },
    handleFilterClick:function(event){
        event.preventDefault();
        event.stopImmediatePropagation();
        if(!_.isUndefined(this.views.hostRightPanel.views.detailView)){
            Spawn.router.navigate("#hosts",{trigger:true});
        }
        return this;
    }
});
Spawn.CommandsView = Spawn.SideFilterLayoutView.extend({
    el:"div#commands"
    ,sideFilterClass:Spawn.CommandFilterView
    ,mainViewClass: Spawn.CommandMainView
    ,render:function(){
        this.$el=$(this.el);
        Spawn.SideFilterLayoutView.prototype.render.call(this);
        this.$el.show();
        return this;
    }
});
//document.ready basically
$.cookie.json=true;
if(_.isUndefined($.cookie("username"))){
	Alertify.dialog.prompt("Enter username:",function(str){
		$.cookie("username",{username:$.trim(str)},{expires:365});
		Spawn.init();
	});
}
else{
	Spawn.init();
}
