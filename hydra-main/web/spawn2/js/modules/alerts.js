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
		"modules/datatable",
	    "modules/util",		
       	"text!../../templates/alerts.filter.html",
       	"text!../../templates/alerts.selectable.html",
       	"text!../../templates/alerts.detail.html",       	
       	"backbone"
],
function(
		 app,
		 alertify,
		 DataTable,
		 util,		 
		 alertFilterTemplate, 
		 alertSelectableTemplate,
		 alertDetailTemplate,
		 Backbone
		 ){
    	var Model = Backbone.Model.extend({
    		idAttribute:"alertId",
        	url:function(){return "/alert/get?alertId=" + this.get("alertId");},
        	defaults:{
        		jobIds:"",
        		type:-1,
        		timeout:-1,
        		canaryCheckDates: "",
        		canaryConfigThreshold: "",
        		email:"",
        		description:""},
        	save:function(){
        		var postData = {
        			lastAlertTime:this.get("lastAlertTime"),
        			type:$("#alertType").val(),
        			timeout:this.get("timeout"),
        			delay:this.get("delay"),
        			email:this.get("email"),
        			description:((this.get("description") instanceof Array) ? this.get("description")[0] : this.get("description")),
        			jobIds:this.get("jobIds").split(","),
        			suppressChanges:this.get("suppressChanges"),
        			canaryPath:this.get("canaryPath"),
        			canaryConfigThreshold:this.get("canaryConfigThreshold"),
                    canaryOps:this.get("canaryOps"),
                    canaryRops:this.get("canaryRops"),
                    canaryFilter:this.get("canaryFilter"),
                    sigma:this.get("sigma"),
                    tolerance:this.get("tolerance")
        		};
        		if (!this.isNew()) {
        			postData.alertId= this.get("alertId");
        		}
        		return $.ajax({
        			url: "/alert/save",
        			type: "POST",
        			data: JSON.stringify(postData),
        			dataType: "json",
        			contentType: "application/json"
        		});
        	},
        	delete:function(){
        		var alertId = this.get("alertId");
        		return $.ajax({
        			url: "/alert/delete",
        			type: "POST",
        			data: {alertId:alertId},
        			dataType: "text"
        		});
        	},
        	parse:function(data) {
        	    var jobIdsArray = data.jobIds;
        	    if (jobIdsArray) {
                    data.jobIds = jobIdsArray.join(",");
        	    }
        		return data;
        	}
    	});
    	var Collection = Backbone.Collection.extend({
        	parse:function(collection){
            	var array = new Array(collection.length);
            	_.each(collection,function(model,idx){
               		array[idx]= Model.prototype.parse(model);
            	});
            	return array;
        	},
        	model:Model
    	});
    	var TableView = DataTable.View.extend({
    		initialize:function(options){
        		_.bindAll(this,'handleDeleteButtonClick');
        		options = options || {};
        		this.id = options.id || "alertTable";
        		this.$el.attr("id",this.id);
        		var self=this;
        		var columns = [{
            		"sTitle":"",
            		"sClass":"alert-cb",
            		"sWidth":"5%",
            		"mData": "alertId",
            		"bSearchable":false,
            		"bSortable":false,
            		"mRender":function(val,type,data){
                		if(self.selectedIds[val]){
                    		return "<input checked class='row_selectable' type='checkbox'></input>";
                		}else{
                    		return "<input class='row_selectable' type='checkbox'></input>";
                		}
            		}
        		},
        		{
        			"sTitle":"Alert ID",
            		"mData": "alertId",
            		"sWidth": "18%",
            		"bVisible":true,
            		"bSearchable":true,
            		"mRender":function(val,type,data){
            		    if (!!data.description) {
            		        return "<a href='#alerts/"+encodeURIComponent(val)+"' title='"+data.description.trim()+"'>"+val+"</a>";
            		    } else {
            		        return "<a href='#alerts/"+encodeURIComponent(val)+"'>"+val+"</a>";
            		    }
            		}
        		},        
        		{
        			"sTitle":"Job IDs",
            		"mData": "jobIds",
            		"sWidth": "22%",
            		"bVisible":true,
            		"bSearchable":true,            
        		},
        		{
        			"sTitle":"Type",
            		"mData": "type",
            		"sWidth": "12%",
            		"bVisible":true,
            		"bSearchable":true,
            		"mRender": function(val,type,data) {
            			return util.alertTypes[val] ? util.alertTypes[val] : "Unknown Alert Type";            			
            		}
        		},
        		{
        			"sTitle":"Timeout",
            		"mData": "timeout",
            		"sWidth": "10%",
            		"bVisible":true,
            		"bSearchable":true,
            		"mRender": function(val,type,data) {
            			return isNaN(val) || val <= 0 ? "" : val;
            		}
        		},
        		{
        			"sTitle":"Emails",
            		"mData": "email",
            		"sWidth": "20%",
            		"bVisible":true,
            		"bSearchable":true,
        		},
        		{
        			"sTitle":"Status",
            		"mData": "lastAlertTime",
            		"sWidth": "23%",
            		"bVisible":true,
            		"bSearchable":true,
            		"mRender": function(val, type, data) {
            			return isNaN(val) || val <= 0 ? "Clear" : "Triggered at " + util.convertToDateTimeText(val);
            		}
        		},         		
				];
    			DataTable.View.prototype.initialize.apply(this,[{
    				columns:columns,
        			filterTemplate:alertFilterTemplate,
        			selectableTemplate:alertSelectableTemplate,
        			heightBuffer:80,        	
        			id:this.id,
        			emptyMessage:" ",
        			idAttribute:"alertId"
    			}]);				
			},
			render:function(){
            	DataTable.View.prototype.render.apply(this,[]);
            	this.views.selectable.find("#deleteAlertButton").on("click",this.handleDeleteButtonClick);
            	return this;
        	},
        	handleDeleteButtonClick:function(event){
            	var ids=this.getSelectedIds();
            	_.each(ids,function(id){
                	var model = app.alertCollection.get(id);
                	if(!_.isUndefined(model)){
                		model.delete().done(function(){
                    		app.alertCollection.remove(model.id);
                		}).fail(function(xhr){
                    		alertify.error("Error deleting alert: "+model.id);
                    	});
                	}
            	});
            	alertify.success(ids.length+" alerts deleted.");
            	app.router.navigate("#alerts",{trigger:true});
        	},
    	});
    	var DetailView = Backbone.View.extend({
    		className:'detail-view',
    		template: _.template(alertDetailTemplate),
        	events: {
            	"click #deleteAlertButton":"handleDeleteButtonClick",
        		"click #saveAlertButton":"handleSaveButtonClick",
        		"keyup input":"handleInputKeyUp",
        		"keyup textarea":"handleTextAreaKeyUp"
        	},
        	initialize:function(){},
        	render:function(){
         		var html = this.template({
            		alert:this.model.toJSON(),
            		alertTypes:util.alertTypes,
            		util:util,
         		});
        		this.$el.html(html);
        		$("#alertType").val(this.model.get("type"));
        		$("#alertType").on("change",this.updateFormOptions);
        		this.updateFormOptions();
         		return this;
        	},
        	handleDeleteButtonClick:function(event){
          		var self=this;
          		this.model.delete().done(function(data){
            		alertify.success("Alert deleted successfully.");
               		app.router.navigate("#alerts",{trigger:true});
        		}).fail(function(xhr){
            		alertify.error("Error deleting alert.");
        		});
        	},
        	handleSaveButtonClick:function(event){
            	var self=this,isNew=this.model.isNew();
            	if (!this.verifyConfig()) {
            		alertify.error("Failed to save alert -- config failed verification.")
            		return;
            	}
            	this.model.save().done(function(data){
					if (data.message) {
						alertify.error("Error saving alert: " + data.message);
					} else {
						alertify.success("Alert saved successfully.");
						if (!self.model.get("alertId")) {
							self.model.set("alertId", data.alertId);
							self.model.fetch({
								success: function(model) {
									app.alertCollection.add(model);
									app.alert=undefined;
									var location = window.location.hash;
									location=location.replace("create",data.alertId);
									app.router.navigate(location,{trigger:true});
								},
								error:function(xhr) {
									alertify.error("Error loading alert for: " + data.alertId);
								}
							});
						}
					}
				}).fail(function(xhr){
                    alertify.error("Error saving alert.");
            	});
        	},
        	handleInputKeyUp:function(event){
        		var input=$(event.currentTarget);
        		var name = input.attr("name");
        		var value = input.val();
        		this.model.set(name,value);
        	},
        	handleTextAreaKeyUp:function(event){
        		var txt=$(event.currentTarget);
        		var name = txt.attr("name");
        		var value = txt.val();
        		var jobs = [];
        		_.each(value.split(','),function(job){
            		var trimmed = $.trim(job);
               		if(!_.isEmpty(trimmed)){
                   		jobs.push(trimmed);
               		}
        		});
        		this.model.set(name,jobs);
        	},
        	updateFormOptions:function() {
        		var type = parseInt($("#alertType").val());
        		// Timeouts are only used for rekick/runtime alerts
        		$("#alertTimeout").toggle(type == 2 || type == 3);
        		// Only show canary path config for certain alerts (4, 5, 6, and 8)
        		$("#alertCanaryPathConfig").toggle(type == 4 || type == 5 || type == 8 || type == 6);
        		$("#alertCanaryConfig").toggle(type == 4 || type == 5);
        		// Show query path, ops, rops, and bundle filter fields for canary map filter (6)
        		$("#alertCanaryFilterConfig").toggle(type == 6);
        		$("#alertSigmaConfig").toggle(type == 7 || type == 8);
        		if (type == 4 || type == 8) {
        			$("#canaryPathHint").text("For example, 'split/importantfiles/{{now-1}}/*.gz'. Mesh lookups are performed relative to the gold directories, so do not include jobid/taskid/gold in your path.")
        		} else if (type == 5) {
        			$("#canaryPathHint").text("For example, 'root/ymd/{{now-1}}:+count'. The query should have exactly one '+' (generally, +count) and return a single numeric quantity per task.");
        		}
        	},
        	verifyConfig: function() {
        		var type = parseInt($("#alertType").val());
        		var email = this.model.get("email");
        		var jobIds = this.model.get("jobIds");
        		if (!email || !jobIds) {
        			alertify.error("Please enter an email and at least one jobId for this alert.");
        			return false;
        		}
        		if (email.indexOf("@") == -1) {
        			alertify.error("Alert email field appears invalid -- please include an '@' character.");
        			return false;
        		}
        		if (type == 4 || type == 5 || type == 8) {
        			var canaryPath = this.model.get("canaryPath");
        			if (!canaryPath) {
        				alertify.error("Please fill out canary path field.");
        				return false;
        			}
        			if (type == 5 && (canaryPath.match(/\+/g)||[]).length != 1) {
 						alertify.error("Please include exactly one '+' corresponding to a numeric field (generally, +count) in your canary path field.");
 						return false;
        			}
        		}
        		if (type == 4 || type == 5) {
        			var canaryConfigThreshold = this.model.get("canaryConfigThreshold");
        			if (!canaryConfigThreshold) {
        				alertify.error("Please fill out all canary configuration fields.");
        				return false;
        			}
        		}
        		return true;
        	}
    	});    
    	return {
    		Model:Model,
    		Collection: Collection,
    		TableView: TableView,
    		DetailView: DetailView
    	};
	});
