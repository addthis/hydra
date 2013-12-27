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
    "text!../../templates/jobs.filter.html",
    "text!../../templates/jobs.selectable.html",
    "backbone"
],
function(
    app,
    DataTable,
    util,
    jobsFilterTemplate,
    jobsSelectableTemplate
){
    var States = [
        "IDLE",
        "SCHEDULED",
        "RUNNING",
        "DEGRADED",
        "UNKNOWN",
        "ERROR",
        "REBALANCE"
    ];
    var Labels = [
        "label-default",
        "label-info",
        "label-success",
        "label-inverse",
        "label-inverse",
        "label-danger",
        "label-info"
    ];
    var Model = Backbone.Model.extend({
        defaults:{
            "description": "(no title)",
            "state": "",
            "creator": "",
            "submitTime": "",
            "startTime": "",
            "endTime": "",
            "replicas": "",
            "backups": "",
            "nodes": ""
        },
        parse:function(data){
            data.DT_RowId=data.id;
            data.DT_RowClass='job-row';
            return data;
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/v2/job/list",
        model:Model,
        parse:function(collection){
            var array = new Array(collection.length);
            _.each(collection,function(model,idx){
                array[idx]= Model.prototype.parse(model);
            });
            return array;
        }
    });
    var View = Backbone.View.extend({
        initialize:function(options){
        },
        render:function(){
            this.$el.html("queryable jobs view..");
            return this;
        }
    });
    var TableView = DataTable.View.extend({
        initialize:function(options){
            options = options || {};
            this.id = options.id || "jobsTable";
            this.$el.attr("id",this.id);
            var columns = [
                {
                    "sTitle":"ID",
                    "sClass":"jobs-id",
                    "mData": "id",
                    "sWidth":"10%",
                    "bVisible":true,
                    "bSearchable":true
                },
                {
                    "sTitle":"State",
                    "sClass":"job-state center",
                    "mData": "state",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return "<div class='label "+Labels[val]+"'>"+States[val]+"</div>";
                    },
                    "aDataSort":[2],
                    "aTargets":[3]
                },
                {
                    "sTitle":"Description",
                    "sClass":"jobs-desc",
                    "mData": "description",
                    //"sWidth":"100%",
                    "bVisible":true,
                    "bSearchable":true,
                    "mRender":function(value,type,data){
                        return "<a href='#jobs/"+data.id+"'>"+value+"</a>";
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
                    "sTitle":"Submitted",
                    "sClass":"job-submitTime center",
                    "mData": "submitTime",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDateTimeText(val);
                    }
                },
                {
                    "sTitle":"Started",
                    "sClass":"job-startTime center",
                    "mData": "submitTime",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDateTimeText(val);
                    }
                },
                {
                    "sTitle":"Ended",
                    "sClass":"job-endTime center",
                    "mData": "endTime",
                    "sWidth":"8%",
                    "bSearchable":false,
                    "mRender":function(val){
                        return util.convertToDateTimeText(val);
                    }
                },
                {
                    "sTitle":"Nodes",
                    "sClass":"job-nodes center",
                    "mData": "nodes",
                    "sWidth":"7%",
                    "bSearchable":false
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:jobsFilterTemplate,
                selectableTemplate:jobsSelectableTemplate,
                heightBuffer:80,
                id:this.id,
                emptyMessage:"No queryable jobs detected yet.."
            }]);
        }
    });
    return {
        TableView:TableView,
        Model:Model,
        Collection:Collection
    };
});