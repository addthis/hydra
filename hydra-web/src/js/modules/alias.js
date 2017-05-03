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
    "jquery",
    "underscore",
    "app",
    "alertify",
    "modules/datatable",
    "modules/util",
    "text!../../templates/alias.filter.html",
    "text!../../templates/alias.selectable.html",
    "text!../../templates/alias.detail.html",
    "backbone"
],
function(
    $,
    _,
    app,
    alertify,
    DataTable,
    util,
    aliasFilterTemplate,
    aliasSelectableTemplate,
    aliasDetailTemplate,
    Backbone
    ){
    var Model = Backbone.Model.extend({
        idAttribute:"name"
        ,url:function(){
            return "/alias/get?id="+this.id;
        }
        ,defaults: {
            name:"(no name)",
            jobs:[]
        },
        save:function(){
            var postData = {
                name:this.get("name"),
                jobs:this.get("jobs").join(",")
            };
            return $.ajax({
                url: "/alias/save",
                type: "POST",
                data: postData,
                dataType: "json"
            });
        },
        delete:function(){
            var name = this.get("name");
            var self = this;
            return $.ajax({
                url: "/alias/delete",
                type: "POST",
                data: {name:name},
                dataType: "text"
            });
        },
        parse:function(data){
            data.DT_RowId=data.name;
            data.DT_RowClass='alias-row';
            return data;
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/alias/list",
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
            this.id = options.id || "aliasTable";
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "sTitle":"",
                    "sClass":"alias-cb",
                    "sWidth":"3%",
                    "mData": "name",
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
                    "mData": "name",
                    "bVisible":false,
                    "bSearchable":true
                },
                {
                    "sTitle":"Name",
                    "sClass":"alias-name",
                    "mData": "name",
                    "sWidth":"30%",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return "<a href='#alias/"+encodeURIComponent(val)+"'>"+val+"</a>";
                    }
                },
                {
                    "sTitle":"Jobs",
                    "sClass":"alias-jobs",
                    "mData": "jobs",
                    "sWidth":"67%",
                    "bVisible":true,
                    "bSearchable":true,
                    "mRender":function(val,type,data){
                        var list = "";
                        for(i in val)
                            list += "<a href='#jobs/"+encodeURIComponent(val[i])+"/conf'>"+val[i]+"</a>&nbsp;&nbsp;";
                        return list;
                    }
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:aliasFilterTemplate,
                selectableTemplate:aliasSelectableTemplate,
                heightBuffer:80,
                id:this.id,
                emptyMessage:" ",
                idAttribute:"name"
            }]);
        },
        render:function(){
            DataTable.View.prototype.render.apply(this,[]);
            this.views.selectable.find("#deleteAliasButton").on("click",this.handleDeleteButtonClick);
            return this;
        },
        handleDeleteButtonClick:function(event){
            var ids=this.getSelectedIds();
            _.each(ids,function(id){
                var model = app.aliasCollection.get(id);
                if(!_.isUndefined(model)){
                    model.delete().done(function(){
                        app.aliasCollection.remove(model.id);
                    }).fail(function(xhr){
                        alertify.error("Error deleting alias: "+model.id);
                    });
                }
            });
            alertify.success(ids.length+" aliases deleted.");
        }
    });
    var DetailView = Backbone.View.extend({
        className:'detail-view',
        template: _.template(aliasDetailTemplate),
        events: {
            "click #deleteAliasButton":"handleDeleteButtonClick",
            "click #saveAliasButton":"handleSaveButtonClick",
            "keyup input":"handleInputKeyUp",
            "keyup textarea":"handleTextAreaKeyUp"
        },
        initialize:function(){
        },
        render:function(){
            var html = this.template({                	
                alias:this.model.toJSON()
            });
            this.$el.html(html);
            return this;
        },
        handleDeleteButtonClick:function(event){
            var self=this;
            this.model.delete().done(function(data){
                alertify.success("Alias deleted successfully.");
                app.router.navigate("#alias",{trigger:true});
            }).fail(function(xhr){
                alertify.error("Error deleting alias.");
            });
        },
        handleSaveButtonClick:function(event){
            var self=this,isNew=this.model.isNew();
            this.model.save().done(function(data){
                alertify.success("Alias saved successfully.");
                if(!_.isUndefined(app.aliasCollection.get(data.name))){
                    self.model.set(Model.prototype.parse(data));
                    app.router.navigate("#alias/"+data.name,{trigger:true});
                }else{
                    app.aliasCollection.fetch({
                        success:function(){
                            app.router.navigate("#alias/"+data.name,{trigger:true});
                        }
                    });
                }
            }).fail(function(xhr){
                alertify.error("Error saving alias: "+self.model.id);
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
        }
    });
    return {
        Model:Model,
        Collection: Collection,
        TableView: TableView,
        DetailView: DetailView
    };
});
