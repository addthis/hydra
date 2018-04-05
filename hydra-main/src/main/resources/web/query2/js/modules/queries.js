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
    "../../../spawn2/js/modules/datatable",
    "../../../spawn2/js/modules/util",
    "text!../../templates/query.filter.html",
    "text!../../templates/query.selectable.html",
    "text!../../templates/query.breadcrumbs.html",
    "text!../../templates/query.detail.html",
    "backbone",
    "date"
],
function(
    app,
    DataTable,
    util,
    queryFilterTemplate,
    querySelectableTemplate,
    queryBreadcrumbsTemplate,
    queryDetailTemplate
){
    var States = [
        "FINISHED",
        "WAITING",
        "QUEUED",
        "RUNNING"
    ];
    var Labels = [
        "label-default",
        "label-warning",
        "label-warning",
        "label-success"
    ];
    var Model = Backbone.Model.extend({
        idAttribute:"uuid",
        defaults:{
            alias: "",
            cacheHit: "",
            hostEntries: "",
            hostInfoCount: "",
            hostInfoSet: [],
            job: "",
            lines: "",
            ops: "",
            path: "",
            remoteip: "",
            runTime: "",
            sender: "",
            startTime: "",
            state: ""
        },
        parse:function(data){
            data.hostInfoCount=data.hostInfoSet.length;
            data.DT_RowId=data.uuid;
            data.DT_RowClass='query-row';
            return data;
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/v2/queries/list",
        model: Model,
        parse:function(collection){
            var array = new Array(collection.length);
            _.each(collection,function(model,idx){
                array[idx]= Model.prototype.parse(model);
            });
            return array;
        }
    });
    var DetailView = Backbone.View.extend({
        className:"detail-view",
        initialize:function(options){
        },
        template: _.template(queryDetailTemplate),
        render:function(){
            var html = this.template({
                query:this.model.toJSON(),
                stateText:States[this.model.get("state")],
                stateLabel:Labels[this.model.get("state")]
            });
            this.$el.html(html);
            this.views= {
                table:new HostTable({
                    collection:this.collection
                })
            };
            this.$el.append(this.views.table.$el);
            this.views.table.render();
            return this;
        }
    });
    var TableView = DataTable.View.extend({
        initialize:function(options){
            //_.bindAll(this,'handleDeleteButtonClick');
            options = options || {};
            this.id = options.id || "queryTable";
            this.$el.attr("id",this.id);
            var hostname=window.location.hostname;
            var compact = options.compact;
            var breadcrumbsTemplate = (!_.isUndefined(options.breadcrumbTemplate)?options.breadcrumbTemplate:(compact?queryBreadcrumbsTemplate:querySelectableTemplate));
            var columns = [
                {
                    "sTitle":"UUID",
                    "sClass":"query-uuid",
                    "mData": "uuid",
                    "sWidth":"10%",
                    "bVisible":true,
                    "bSearchable":true,
                    "mRender":function(value,type,data){
                        return "<a href='#queries/"+value+"'>"+value+"</a>";
                    }
                },
                {
                    "sTitle":"State",
                    "sClass":"query-state center",
                    "mData": "state",
                    "sWidth":"8%",
                    "bVisible":true,
                    "bSearchable":true,
                    "mRender":function(value,type,data){
                        return "<div class='label "+Labels[value]+"'>"+States[value]+"</div>";
                    }
                },
                {
                    "sTitle":"Start",
                    "sClass":"query-start",
                    "mData": "startTime",
                    //"sWidth":"100%",
                    "bVisible":!compact,
                    "bSearchable":false,
                    "mRender":function(value,type,data){
                        return util.convertToDateTimeText(value);
                    }
                },
                {
                    "sTitle":"Runtime",
                    "sClass":"query-runtime",
                    "mData": "runTime",
                    //"sWidth":"100%",
                    "bVisible":!compact,
                    "bSearchable":true
                },
                {
                    "sTitle":"Sender",
                    "sClass":"query-sender",
                    "mData": "sender",
                    //"sWidth":"100%",
                    "bVisible":!compact,
                    "bSearchable":true
                },
                {
                    "sTitle":"Remote IP",
                    "sClass":"query-remoteip",
                    "mData": "remoteip",
                    //"sWidth":"100%",
                    "bVisible":!compact,
                    "bSearchable":true
                },
                {
                    "sTitle":"Hosts",
                    "sClass":"query-hosts",
                    "mData": "hostEntries",
                    //"sWidth":"100%",
                    "bVisible":!compact,
                    "bSearchable":false,
                    "mRender":function(value,type,data){
                        return (data.cacheHit?'[cache]':value);
                    }
                },
                {
                    "sTitle":"Job",
                    "sClass":"query-job",
                    "mData": "job",
                    //"sWidth":"100%",
                    "bVisible":!compact,
                    "bSearchable":true,
                    "mRender":function(value,type,data){
                        return "<a href='http://"+hostname+":5052/spawn2/index.html#jobs/"+value+"/conf' target='_blank'>"+value+"</a>";
                    }
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:(compact?"":queryFilterTemplate),
                selectableTemplate:breadcrumbsTemplate,
                heightBuffer:80,
                id:this.id,
                emptyMessage:"No queries yet..",
                enableSearch:!compact
            }]);
        }
    });
    var HostModel = Backbone.Model.extend({
        defaults:{
            "hostname": "",
            "lines": "",
            "startTime": "",
            "endTime": "",
            "taskId": "",
            "finished": "",
            "runtime":""
        },
        parse:function(data){
            var id = data.hostname+data.taskId;
            data.id=id;
            data.DT_RowId=id;
            data.DT_RowClass='host-row';
            if(data.endTime===0){
                data.runtime=(new Date().getTime())-data.startTime;
            }
            else{
                data.runtime=data.endTime-data.startTime;
            }
            return data;
        }
    });
    var HostCollection = Backbone.Collection.extend({
        initialize:function(){
            _.bindAll(this,'url','parse');
        },
        url:function(){
            return "/v2/host/list?uuid="+this.uuid;
        },
        parse:function(collection){
            var array = new Array(collection.length);
            _.each(collection,function(model,idx){
                array[idx]= HostModel.prototype.parse(model);
            });
            return array;
        },
        model: HostModel
    });
    var HostTable = DataTable.View.extend({
        initialize:function(options){
            options = options || {};
            this.id = options.id || "queryHostTable"+this.collection.uuid;
            this.$el.attr("id",this.id);
            var columns = [
                {
                    "sTitle":"Task",
                    "sClass":"host-task",
                    "mData": "taskId",
                    "bVisible":true,
                    "bSearchable":true,
                    "sWidth":"7%"
                },
                {
                    "sTitle":"Host",
                    "sClass":"host-hostname",
                    "mData": "hostname",
                    //"sWidth":"100%",
                    "bVisible":true,
                    "bSearchable":true,
                    "sWidth":"20%",
                    "mRender":function(value,type,data){
                        return "<a href='#hosts/"+value+"'>"+value+"</a>";
                    }
                },
                {
                    "sTitle":"Status",
                    "sClass":"host-status",
                    "mData": "finished",
                    "sWidth":"10%",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(finished,type,data){
                        var text = (finished?"FINISHED":"RUNNING");
                        var label = (finished?"label-default":"label-success");
                        return "<div class='label "+label+"'>"+text+"</div>";
                    }
                },
                {
                    "sTitle":"Lines",
                    "sClass":"host-lines",
                    "mData": "lines",
                    "sWidth":"10%",
                    "bVisible":true,
                    "bSearchable":false
                },
                {
                    "sTitle":"Runtime (ms)",
                    "sClass":"host-runtime",
                    "mData": "runtime",
                    "sWidth":"15%",
                    "bVisible":true,
                    "bSearchable":false
                },
                {
                    "sTitle":"Start",
                    "sClass":"host-startTime",
                    "mData": "startTime",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(value,type,data){
                        return util.convertToDateTimeText(value);
                    }
                },
                {
                    "sTitle":"End",
                    "sClass":"host-endTime",
                    "mData": "endTime",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(value,type,data){
                        return util.convertToDateTimeText(value);
                    }
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:"",
                selectableTemplate:"",
                heightBuffer:80,
                id:this.id,
                emptyMessage:"No query hosts yet.."
            }]);
        }
    });
    return {
        Model:Model,
        Collection:Collection,
        TableView:TableView,
        DetailView:DetailView,
        HostModel: HostModel,
        HostCollection: HostCollection
    };
});