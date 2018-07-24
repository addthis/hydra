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
    "text!../../templates/mqworker.filter.html",
    "text!../../templates/mqworker.selectable.html",
    "backbone"
],
function(
    app,
    DataTable,
    util,
    mqworkerFilterTemplate,
    mqworkerSelectableTemplate
){
    var States=[
        "CONNECTED",
        "DISCONNECTED",
        "WAITING",
        "UNKNOWN"
    ];
    var Labels=[
        "label-success",
        "label-danger",
        "label-info",
        "label-inverse"
    ];
    var Model = Backbone.Model.extend({
        idAttribute:"uuid",
        defaults:{
            "host": "",
            "port": "",
            "lastModified": "",
            "state": "",
            "metrics": {}
        },
        parse:function(data){
            data.DT_RowId=data.uuid;
            data.DT_RowClass="host-row";
            return data;
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/v2/hosts/list",
        model: Model,
        parse:function(collection){
            var array = new Array(collection.length);
            _.each(collection,function(model,idx){
                array[idx]= Model.prototype.parse(model);
            });
            return array;
        }
    });
    var TableView = DataTable.View.extend({
        initialize:function(options){
            options = options || {};
            this.id = options.id || "mqworkerTable";
            this.$el.attr("id",this.id);
            var columns = [
                {
                    "sTitle":"Host",
                    "sClass":"mqworker-host",
                    "mData": "host",
                    "sWidth":"15%",
                    "bVisible":true,
                    "bSearchable":true,
                    "mRender":function(value,type,data){
                        return "<a href='#hosts/"+data.uuid+"'>"+value+"</a>";
                    }
                },
                {
                    "sTitle":"State",
                    "sClass":"mqworker-state",
                    "mData": "state",
                    "sWidth":"10%",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(value,type,data){
                        return "<div class='label "+Labels[value]+"'>"+States[value]+"</div>";
                    }
                },
                {
                    "sTitle":"UUID",
                    "sClass":"mqworker-uuid",
                    "mData": "uuid",
                    //"sWidth":"100%",
                    "bVisible":true,
                    "bSearchable":true
                },
                {
                    "sTitle":"Last Modified",
                    "sClass":"mqworker-lastModified",
                    "mData": "lastModified",
                    //"sWidth":"100%",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(value,type,data){
                        return util.convertToDateTimeText(value);
                    }
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:mqworkerFilterTemplate,
                selectableTemplate:mqworkerSelectableTemplate,
                heightBuffer:80,
                id:this.id,
                emptyMessage:"No mqworkers detected yet.."
            }]);
        }
    });
    var View = Backbone.View.extend({
        initialize:function(options){
        },
        render:function(){
            this.$el.html("mqworkers view..");
            return this;
        }
    });
    return {
        Model:Model,
        Collection:Collection,
        TableView:TableView,
        View:View
    };
});