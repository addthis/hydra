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
    "modules/editor",
    "text!../../templates/macro.filter.html",
    "text!../../templates/macro.selectable.html",
    "text!../../templates/macro.detail.html",
    "text!../../templates/macro.configuration.html",
    "backbone"
],
function(
    app,
    alertify,
    DataTable,
    util,
    Editor,
    macroFilterTemplate,
    macroSelectableTemplate,
    macroDetailTemplate,
    macroConfigurationTemplate,
    Backbone
){
    var Model = Backbone.Model.extend({
        idAttribute:"name"
        ,url:function(){
            return "/macro/get?label="+this.id;
        },
        defaults: {
            modified:"",
            owner:"none",
            description:"(no description)",
            macro:"        \n"
        },
        parse:function(data){
            data.DT_RowId=data.name;
            data.DT_RowClass='macro-row';
            return data;
        },
        delete:function(){
            var self = this;
            return $.ajax({
                url: "/macro/delete",
                type: "POST",
                data: {name:self.id},
                dataType:"text"
            });
        },
        save:function(){
            var self=this;
            var data = this.toJSON();
            data.label=data.name;
            data.owner=app.user.get("username");
            return $.ajax({
                url: "/macro/save",
                type: "POST",
                data: data,
                dataType:"json"
            });
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/macro/list",
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
            this.id = options.id || "macroTable";
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "sTitle":"",
                    "sClass":"macro-cb",
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
                    "sClass":"macro-name",
                    "mData": "name",
                    "sWidth":"25%",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return "<a href='#macros/"+encodeURIComponent(val)+"'>"+val+"</a>";
                    }
                },
                {
                    "sTitle":"Modified",
                    "sClass":"macro-modified",
                    "sType": "date",
                    "mData": "modified",
                    "sWidth":"10%",
                    "mRender":function(val,type,data){
                        return util.convertToDateTimeText(val);
                    }
                },
                {
                    "sTitle":"Owner",
                    "sClass":"macro-owner",
                    "mData": "owner",
                    "sWidth":"10%"
                },
                {
                    "sTitle":"Description",
                    "sClass":"macro-description",
                    "mData": "description",
                    "sWidth":"53%"
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:macroFilterTemplate,
                selectableTemplate:macroSelectableTemplate,
                heightBuffer:80,
                id:this.id,
                emptyMessage:" ",
                idAttribute:"name"
            }]);
        },
        render:function(){
            DataTable.View.prototype.render.apply(this,[]);
            this.views.selectable.find("#deleteMacroButton").on("click",this.handleDeleteButtonClick);
            return this;
        },
        handleDeleteButtonClick:function(event){
            var ids=this.getSelectedIds();
            _.each(ids,function(id){
                var model = app.macroCollection.get(id);
                if(!_.isUndefined(model)){
                    model.delete().done(function(){
                        app.macroCollection.remove(model.id);
                    }).fail(function(xhr){
                        alertify.error("Error deleting '" + model.id + "': " + xhr.responseText);
                    });
                }
            });
            alertify.message("Deleting " + ids.length + " macros...");
        }
    });
    var DetailView = Backbone.View.extend({
        className:"detail-view",
        initialize:function(){
            _.bindAll(this,'render','template','handleDeleteButtonClick','handleSaveButtonClick');

        },
        template: _.template(macroDetailTemplate),
        events: {
            "click #deleteMacroButton":"handleDeleteButtonClick",
            "click #saveMacroButton":"handleSaveButtonClick",
            "keyup input":"handleInputKeyUp"
        },
        render:function(){
            var html = this.template({
                macro:this.model.toJSON(),
                util:util
            });
            this.$el.html(html);
            this.views = {
                editor: new Editor.AceView({
                    model:this.model,
                    keyName:"macro"
                }).render()
            };
            //adjust height
            this.views.editor.$el.css({
                position:"absolute",
                top:'59px',
                bottom:0,
                right:0,
                left:0
            })
            this.$el.find("div#detailContainer").append(this.views.editor.$el);
            return this;
        },
        handleDeleteButtonClick:function(event){
            var self=this;
            this.model.delete().done(function(data){
                alertify.success("Macro deleted successfully.");
                app.router.navigate("#macros",{trigger:true});
            }).fail(function(xhr){
                alertify.error("Error deleting macro.");
            });
        },
        handleSaveButtonClick:function(event){
            var self=this,isNew=this.model.isNew();
            this.model.save().done(function(data){
                alertify.success("Macro saved successfully.");
                app.macroCollection.fetch({
                    success:function(){
                        app.router.navigate("#macros/"+data.name,{trigger:true});
                    }
                });
            }).fail(function(xhr){
                alertify.error("Error saving macro: "+self.model.id);
            });
        },
        handleInputKeyUp:function(event){
            var input=$(event.currentTarget);
            var name = input.attr("name");
            var value = input.val();
            this.model.set(name,value);
        }
    });
    return {
        Model:Model,
        Collection: Collection,
        TableView: TableView,
        DetailView: DetailView
    };
});
