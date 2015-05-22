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
    "alertify",
    "text!../../templates/settings.rebalance.html",
    "backbone"
],
function(
    alertify,
    settingsRebalanceTemplate
){
    var RebalanceModel = Backbone.Model.extend({
        url:"/system/balance.params.get",
        defaults:{
            autoBalanceLevel:"",
            bytesMovedFullRebalance:"",
            hostAutobalanceIntervalMillis:"",
            jobAutobalanceIntervalMillis:"",
            tasksMovedFullRebalance:""
        },
        save:function(){
            var self=this;
            return $.ajax({
                url: "/system/balance.params.set",
                type: "GET",
                data: {
                    params:JSON.stringify(self.toJSON())
                },
                dataType:"text"
            });
        }
    });
    var RebalanceView = Backbone.View.extend({
        template: _.template(settingsRebalanceTemplate),
        events:{
            "click #saveButton":"handleSaveButton",
            "click #resetButton":"handleResetButton"
        },
        initialize:function(){
            this.listenTo(this.model,"change",this.render);
        },
        render:function(){
            var html = this.template(this.model.toJSON());
            this.$el.html(html);
            return html;
        },
        handleResetButton:function(event){
            var self=this;
            this.model.fetch({
                success:function(){
                    self.render();
                }
            });
        },
        handleSaveButton:function(event){
            var data = this.getFormValues();
            this.model.set(data);
            this.model.save().done(function(data){
                alertify.success("Rebalance params saved successfully.");
            }).fail(function(xhr){
                alertify.error(xhr.responseText);
            });
        },
        getFormValues:function(){
            var inputs = this.$el.find("input"), self=this;
            var data={};
            _.each(inputs,function(input){
                var $input = $(input);
                var name = $input.attr("name");
                var value = $input.val();
                data[name]=value;
            });
            return data;
        }
    });
    return {
        RebalanceModel:RebalanceModel,
        RebalanceView:RebalanceView
    };
});