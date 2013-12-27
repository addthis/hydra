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
    "modules/datatable",
    "modules/task",
    "modules/util",
    "text!../../templates/host.filter.html",
    "text!../../templates/host.selectable.html",
    "text!../../templates/host.detail.html",
    "backbone"
],
function(
    app,
    DataTable,
    Task,
    util,
    hostFilterTemplate,
    hostSelectableTemplate,
    hostDetailTemplate,
    Backbone
){
    var Model = Backbone.Model.extend({
        idAttribute:"uuid",
        parse:function(data){
            data.DT_RowId=data.uuid;
            data.DT_RowClass='host-row';
            data.host= data.host || "";
            data.port= data.port || "";
            data.minionTypes= data.minionTypes || "";
            data.runCount = data.running.length;
            data.queuedCount = data.queued.length;
            data.total=data.total || 0;
            if(_.isEmpty(data.max)){
                data.max= {
                    cpu: "",
                    disk: "",
                    io: "",
                    mem:""
                };
            }
            if(_.isEmpty(data.used)){
                data.used={
                    cpu: "",
                    disk: "",
                    io: "",
                    mem: ""
                };
            }
            data.diskUsed = data.used.disk;
            data.diskMax = data.max.disk;
            data.availableTaskSlots=data.availableTaskSlots;
            return data;
        },
        defaults:{
            "availableTaskSlots": "",
            "runCount":0,
            "queuedCount":0,
            "backingup": [],
            "dead": "",
            "disabled": "",
            "diskReadOnly": "",
            "group": "",
            "histQueueSize": "",
            "histWaitTime": "",
            "host": "",
            "jobRuntimes": {},
            "lastUpdateTime": "",
            "max": {
                "cpu": "",
                "disk": "",
                "io": "",
                "mem":""
            },
            "meanActiveTasks": "",
            "minionTypes": "",
            "path": "",
            "port": "",
            "queued": [],
            "readOnly": "",
            "replicating": [],
            "running": [],
            "score": "",
            "stopped": "",
            "total":0,
            "time": "",
            "up": "",
            "uptime": "",
            "used": {
                "cpu": "",
                "disk": "",
                "io": "",
                "mem": ""
            },
            "user": "",
            "uuid": ""
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/host/list",
        initialize:function(){
            this.listenTo(app.server,"host.update",this.handleHostUpdate);
        },
        parse:function(collection){
            var array = new Array(collection.length);
            _.each(collection,function(model,idx){
                array[idx]= Model.prototype.parse(model);
            });
            return array;
        },
        handleHostUpdate:function(data){
            var host = this.get(data.uuid);
            if(!_.isUndefined(host)){
                host.set(
                    Model.prototype.parse(data)
                );
            }
            else{
                host=new Model(
                    Model.prototype.parse(data)
                );
                this.add([host]);
            }
        },
        model:Model,
        rebalanceSelected:function(hostIds){
            var count = hostIds.length;
            $.ajax({
                url: "/host/rebalance?id="+hostIds,
                type: "GET",
                dataType: "json"
            }).done(function(data){
                Alertify.log.info(count+" host(s) rebalanced.",2000)
            }).fail(function(e){
                Alertify.log.error("Error rebalancing: "+count+" hosts. <br/> "+e.responseText);
            });
        },
        failSelected:function(hostIds, deadFs){
            var count = hostIds.length;
            $.ajax({
                url: "/host/fail?id="+hostIds+"&deadFs="+deadFs,
                type: "GET",
                dataType: "json"
            }).done(function(data){
                Alertify.log.info(count+" host(s) failed.",2000)
            }).fail(function(e){
                Alertify.log.error("Error failing: "+count+" hosts. <br/> "+e.responseText);
            });
        },
        dropSelected:function(self, hostIds){
            var count = hostIds.length;
            $.ajax({
                url: "/host/drop?id="+hostIds,
                type: "GET",
                dataType: "text"
            }).done(function(data){
                self.collection.fetch();
                Alertify.log.info(count+" host(s) dropped.",2000);
            }).fail(function(e){
                Alertify.log.error("Error dropping: "+count+" hosts. <br/> "+e.responseText);
            });
        },
        toggleSelected:function(hostIds,disable){
            var count = hostIds.length;
            $.ajax({
                url: "/host/toggle?hosts="+hostIds+"&disable="+disable,
                type: "GET",
                dataType: "text"
            }).done(function(data){
                Alertify.log.success(count+" host(s) "+(disable?"dis":"en")+"abled successfully",2000);
            }).fail(function(e){
                Alertify.log.error("Error "+(disable?"dis":"en")+"abling: "+count+" hosts. <br/> "+e.responseText);
            });
        },
        getFailInfo:function(self, ids, deadFs){
            $.ajax({
                url: "/host/failinfo?id="+ids+"&deadFs="+(deadFs ? 1 : 0),
                type: "GET",
                dataType: "json"
            }).done(function(data){
                uuids = data.uuids;
				if (data.fatal){
					Alertify.log.error("Fatal warning for failing " + uuids + ": " + data.fatal + "; fail aborted");
					return;
				}
				var msg = "Are you sure you want to fail " + uuids + "?\n";
				msg += "After failing, cluster will go from " + util.formatPercent(data.prefail) + "% disk used to " + util.formatPercent(data.postfail) + "%.\n";	
				if (data.warning){
					msg += "Warning: " + data.warning;
				}
				Alertify.dialog.confirm(msg,
                	function(resp){
                    	self.collection.failSelected(ids, data.deadFs);
                	}
            	);
            }).fail(function(e){
                Alertify.log.error("Error trying to fail: "+ids+" hosts. <br/> "+e.responseText);
            });
        },
        cancelFailSelected:function(self, ids){
        	$.ajax({
        		url: "/host/failcancel?id="+ids,
        		type: "GET",
        		dataType: "text"
        	}).done(function(data){
        		Alertify.log.success(ids.length+" host(s) removed from failure queue");
        	}).fail(function(e){
        		Alertify.log.error("Error trying to remove: "+ids+" from failure queue. <br/> "+e.responseText);
        	});
        }
    });
    var TableView = DataTable.View.extend({
        initialize:function(options){
            _.bindAll(this,'render','handleRebalanceButtonClick','handleCancelFailButtonClick','handleFailFsOkayButtonClick','handleFailFsDeadButtonClick','handleDropButtonClick','handleEnableButtonClick','handleDisableButtonClick');
            options = options || {};
            this.id = options.id || "hostTable";
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "sTitle":"",
                    "sClass":"host-cb",
                    "sWidth":"3%",
                    "mData": "uuid",
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
                    "sTitle":"Host",
                    "sClass":"host-host",
                    "mData": "host"
                },
                {
                    "sTitle":"Port",
                    "sClass":"host-port",
                    "mData": "port"
                },
                {
                    "mData": "uuid",
                    "bVisible":false,
                    "bSearchable":true
                },
                {
                    "sTitle":"UUID",
                    "sClass":"host-uuid",
                    "mData": "uuid",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return "<a href='#hosts/"+val+"'>"+val+"</a>";
                    },
                    "aDataSort":[3],
                    "aTargets":[4]
                },
                {
                    "sTitle":"Type",
                    "sClass":"host-minionTypes",
                    "mData": "minionTypes"
                },
                {
                    "sTitle":"State",
                    "sClass":"host-state center",
                    "mData": "spawnState",
                    "bSearchable":true,
                },
                {
                    "sTitle":"Group",
                    "sClass":"host-group center",
                    "mData": "group"
                },
                {
                    "sTitle":"Score",
                    "sClass":"host-score center",
                    "mData": "score",
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return Math.round(val);
                    }
                },
                {
                    "sTitle":"Queued",
                    "sClass":"host-queued center",
                    "mData": "queuedCount",
                    "bSearchable":false
                },
                {
                    "sTitle":"Running",
                    "sClass":"host-running center",
                    "mData": "runCount",
                    "bSearchable":false
                },
                {
                    "sTitle":"Live",
                    "sClass":"host-total center",
                    "mData": "total",
                    "bSearchable":false
                },
            	{
                    "sTitle":"Disk",
                    "sClass":"host-disk center",
                    "mData": "used.disk",
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return util.convertToDFH(val)+" / "+util.convertToDFH(data.max.disk);
                    }
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:hostFilterTemplate,
                selectableTemplate:hostSelectableTemplate,
                heightBuffer:80,
                columnFilterIndex:5,
                id:this.id,
                changeAttrs:[
                    "runCount",
                    "queuedCount",
                    "total",
                    "diskUsed",
                    "host",
                    "port",
                    "up",
                    "score",
                    "spawnState",
                    "diskMax",
                    "disabled",
                    "down",
                    "dead"
                ]
            }]);
            this.hasRendered=false
        },
        render:function(){
            DataTable.View.prototype.render.apply(this,[]);
            if(!this.hasRendered){
                this.views.selectable.find("#hostRebalanceButton").on("click",this.handleRebalanceButtonClick);
                this.views.selectable.find("#hostFailFsOkayButton").on("click",this.handleFailFsOkayButtonClick);
                this.views.selectable.find("#hostFailFsDeadButton").on("click",this.handleFailFsDeadButtonClick);
                this.views.selectable.find("#hostFailCancelButton").on("click",this.handleCancelFailButtonClick);
                this.views.selectable.find("#hostDropButton").on("click",this.handleDropButtonClick);
                this.views.selectable.find("#hostEnableButton").on("click",this.handleEnableButtonClick);
                this.views.selectable.find("#hostDisableButton").on("click",this.handleDisableButtonClick);
                this.hasRendered=true;
            }
        },
        handleRebalanceButtonClick:function(event){
            var ids = this.getSelectedIds();
            this.collection.rebalanceSelected(ids);
        },
        handleFailFsOkayButtonClick:function(event){
            var ids = this.getSelectedIds(), self=this;
            this.collection.getFailInfo(self, ids, false);
        },
        handleFailFsDeadButtonClick:function(event){
            var ids = this.getSelectedIds(), self=this;
            this.collection.getFailInfo(self, ids, true);        
        },
        handleCancelFailButtonClick:function(event){
        	var ids = this.getSelectedIds(), self=this;
        	this.collection.cancelFailSelected(self, ids);
        },
        handleDropButtonClick:function(event){
            var ids = this.getSelectedIds(),self=this;
            Alertify.dialog.confirm("Are you sure you would like to DROP "+ids.length+" host(s): "+ids.join(",")+"?",
                function(resp){
                    self.collection.dropSelected(self, ids);
                }
            );
        },
        handleEnableButtonClick:function(event){
            var ids = this.getSelectedIds();
            this.collection.toggleSelected(ids,false);
        },
        handleDisableButtonClick:function(event){
            var ids = this.getSelectedIds();
            this.collection.toggleSelected(ids,true);
        }
    });
    var DetailView = Backbone.View.extend({
        className:"detail-view",
        template: _.template(hostDetailTemplate),
        initialize:function(){
        },
        render:function(){
            var html = this.template({
                host:this.model.toJSON()
            });
            this.$el.html(html);
            return this;
        }
    });
    var TaskDetailView = DetailView.extend({
        initialize:function(){
            this.listenTo(app.server,'job.update',this.handleJobUpdate);
        },
        render:function(){
            DetailView.prototype.render.apply(this,[]);
            this.views={
                table: new Task.TableView({
                    id:'jobTaskTable',
                    collection:this.collection
                })
            };
            var detail = this.$el.find("div#detailContainer");
            detail.append(this.views.table.$el);
            this.views.table.render();
            this.$el.find("ul.nav.nav-tabs li#tasksTab").addClass("active");
            return this;
        },
        handleJobUpdate:function(job){
            var tasks = this.collection.where({"jobUuid":job.id});
            _.each(tasks,function(task){
                task.fetch();
            });
        },
        handleHostUpdate:function(){
            _.each(this.collection,function(task){
                task.fetch();
            });
        }
    });
    return {
        Model:Model,
        Collection: Collection,
        TableView: TableView,
        DetailView:DetailView,
        TaskDetailView: TaskDetailView
    };
})
