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
       "text!../../templates/git.properties.html",
       "backbone"
],
function(gitPropertiesTemplate){
    var Git={};
    Git.Model = Backbone.Model.extend({
        url:"/system/git.properties",
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
    Git.PropertiesView = Backbone.View.extend({
        events:{
        },
        template:_.template(gitPropertiesTemplate),
        initialize:function(options){
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
    return Git;
});
