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
    "modules/util",
    "text!../../templates/command.filter.html",
    "text!../../templates/command.selectable.html",
    "text!../../templates/command.detail.html",
    "backbone"
],
function(
    app,
    DataTable,
    util,
    commandFilterTemplate,
    commandSelectableTemplate,
    commandDetailTemplate,
    Backbone
){
    var Model = Backbone.Model.extend({
        idAttribute:"name"
        ,url:function(){
            return "/command/get?command="+this.id;
        },defaults: {
            name:"(no name)",
            command:[],
            owner:"none",
            reqCPU:1,
            reqIO:1,
            reqMEM:512
        },
        save:function(){
            var data = this.toJSON();
            data.command = data.command.join(",");
            return $.ajax({
                url: "/command/save",
                type: "POST",
                data: data,
                dataType: "json"
            });
            /*

             success: function(data){
             Alertify.log.success("Command saved successfully.")
             self.set(data);
             Spawn.commandCollection.add(data);
             Spawn.router.navigate("#commands/"+data.name,{trigger:true});
             },
             error: function(){
             Alertify.log.error("Error saving command.");
             },
            * */
        },
        delete:function(){
            var name = this.get("name");
            var self = this;
            return $.ajax({
                url: "/command/delete",
                type: "POST",
                data: {name:name},
                dataType:"text"
            });
            /**,
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
             dataType: "json"*/
        },
        parse:function(data){
            data.DT_RowId=data.name;
            data.DT_RowClass='command-row';
            return data;
        }
    });
    var Collection = Backbone.Collection.extend({
        url:"/command/list",
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
            this.id = options.id || "commandTable";
            this.$el.attr("id",this.id);
            var self=this;
            var columns = [
                {
                    "sTitle":"",
                    "sClass":"command-cb",
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
                    "sClass":"command-name",
                    "mData": "name",
                    "sWidth":"30%",
                    "bVisible":true,
                    "bSearchable":false,
                    "mRender":function(val,type,data){
                        return "<a href='#commands/"+encodeURIComponent(val)+"'>"+val+"</a>";
                    }
                },
                {
                    "sTitle":"Command",
                    "sClass":"command-command",
                    "mData": "command",
                    "sWidth":"67%",
                    "bVisible":true,
                    "bSearchable":true,
                    "mRender":function(val,type,data){
                        return val.join(",");
                    }
                }
            ];
            DataTable.View.prototype.initialize.apply(this,[{
                columns:columns,
                filterTemplate:commandFilterTemplate,
                selectableTemplate:commandSelectableTemplate,
                heightBuffer:80,
                //columnFilterIndex:5,
                id:this.id,
                emptyMessage:" ",
                idAttribute:"name"
            }]);
        },
        render:function(){
            DataTable.View.prototype.render.apply(this,[]);
            this.views.selectable.find("#deleteCommandButton").on("click",this.handleDeleteButtonClick);
            return this;
        },
        handleDeleteButtonClick:function(event){
            var ids=this.getSelectedIds();
            _.each(ids,function(id){
                var model = app.commandCollection.get(id);
                if(!_.isUndefined(model)){
                    model.delete().done(function(){
                        app.commandCollection.remove(model.id);
                    }).fail(function(xhr){
                        Alertify.log.error("Error deleting command: "+model.id);
                    });
                }
            });
            Alertify.log.success(ids.length+" commands deleted.");
        }
    });
    var DetailView = Backbone.View.extend({
        className:"detail-view",
        template: _.template(commandDetailTemplate),
        events: {
            "click #deleteCommandButton":"handleDeleteButtonClick",
            "click #saveCommandButton":"handleSaveButtonClick",
            "keyup input":"handleInputKeyUp",
            "keyup textarea":"handleTextAreaKeyUp"
        },
        initialize:function(){
        },
        render:function(){
            var html = this.template({
                command:this.model.toJSON()
            });
            this.$el.html(html);
            return this;
        },
        handleDeleteButtonClick:function(event){
            var self=this;
            this.model.delete().done(function(data){
                Alertify.log.success("Command deleted successfully.");
                app.router.navigate("#commands",{trigger:true});
            }).fail(function(xhr){
                Alertify.log.error("Error deleting command.");
            });
        },
        handleSaveButtonClick:function(event){
            var self=this,isNew=this.model.isNew();
            this.model.save().done(function(data){
                Alertify.log.success("Command saved successfully.");
                if(!_.isUndefined(app.commandCollection.get(data.name))){
                    self.model.set(Model.prototype.parse(data));
                    app.router.navigate("#commands/"+data.name,{trigger:true});
                }else{
                    app.commandCollection.fetch({
                        success:function(){
                            app.router.navigate("#commands/"+data.name,{trigger:true});
                        }
                    });
                }
            }).fail(function(xhr){
                Alertify.log.error("Error saving command: "+self.model.id);
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
            var commands = [];//value.split('\n');
            _.each(value.split('\n'),function(comm){
                var trimmed = $.trim(comm);
                if(!_.isEmpty(trimmed)){
                    commands.push(trimmed);
                }
            });
            this.model.set("command",commands);
        }
    });
    return {
        Model:Model,
        Collection: Collection,
        TableView: TableView,
        DetailView: DetailView
    };
});
