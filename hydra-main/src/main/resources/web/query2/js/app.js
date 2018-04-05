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
    "router",
    "../../spawn2/js/modules/server",
    "jquery.cookie",
    "jquery",
    "underscore",
    "backbone",
    "jquery.cookie",
    "alertify"
],
function(
    Router,
    server
){
    $.cookie.json=true;
    var app = {
        router: new Router(),
        cookieExpires:7,
        currentView:null,
        mainSelector:"#main",
        user: new Backbone.Model({username:""}),
        server:server,
        activeModels:[],
        setCookie:function(name,value){
            $.cookie(name,value,{
                expired:this.cookieExpires
            });
        },
        getCookie:function(name){
            return $.cookie(name);
        },
        showView:function(view,link,activeModels){
            var self=this;
            if(!_.isNull(this.currentView)){
                if(_.has(this.currentView,'close')){
                    this.currentView.close();
                }else{
                    this.currentView.remove();
                }
            }
            this.currentView=view;
            $(this.mainSelector).append(this.currentView.$el);
            this.currentView.render();
            $("div.navbar ul.nav.navbar-nav li a").parent().removeClass("active");
            $("div.navbar ul.nav.navbar-nav li a[href*='"+link+"']").parent().addClass("active");
            $("div.navbar ul.nav.navbar-nav li a[href*='"+link+"']").closest("li.dropdown-toggle").addClass("active");
            activeModels=activeModels || [];
            _.each(activeModels,function(modelName){
                self[modelName]=undefined;
            });
        },
        authenticate:function(){
            var username = $.cookie("username"), self=this;
            if(_.isUndefined(username)){
                var alert = Alertify.dialog.prompt("Enter username:",function(str){
                    var data = {username: $.trim(str)};
                    $.cookie("username",data,{expires:365});
                    self.user.set("username", $.trim(str));
                });
                $(alert.el).find("#alertify-text").focus();
            }
            else{
                self.user.set("username",username.username);
            }
        },
        makeHtmlTitle:function(title){
            var hostname = location.hostname;
            var index = hostname.indexOf(".");
            if(index >= 0){
                hostname = hostname.substring(0, index);
            }
            var title = hostname + " " + title;
            document.title=title;
        },
        log:function(text){
            var date = new Date(Date.now());
            console.log(date.toString("hh:mm:ss")+" - "+text);
        }
    };
    return app;
});