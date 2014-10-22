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

//app.js is your application-level namespace where you instantiate all your top-level application level function, etc.. no router logic should go here
define([
    "router",
    "modules/server",
    "jquery.cookie",
    "jquery",
    "underscore",
    "backbone",
    "jquery.cookie",
    "alertify"
],
function(
    Router,
    server,
    User
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
            //update navbar link
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
        healthCheck:function(){
            $.ajax({
                url: "/system/healthcheck",
                type: "GET"
            }).done(function(data){
                if (data) {
                    Alertify.log.info("Health check passed");
                } else {
                    Alertify.dialog.alert("Health check failed! Make sure Spawn data store is up-to-date!");
                }
            });
        },
        isQuiesced:false,
        quiesce:function(){
            var self=this;
            Alertify.dialog.confirm( ((this.isQuiesced?"un":"")+"quiesce the cluster? (if you don't know what you're doing, hit cancel!)"), function (e) {
                $.ajax({
                    url: "/system/quiesce",
                    type: "GET",
                    data: {quiesce:(self.isQuiesced?"0":"1")}
                }).done(function(data){
                    Alertify.log.info("Cluster "+(data.quiesced=="1"?"quiesced":"reactivated")+" successfully.");
                    self.isQuiesced= !self.isQuiesced;
                    self.checkQuiesced();
                }).fail(function(){
                    Alertify.dialog.alert("You do not have sufficient privileges to quiesce cluster");
                });
            });
        },
        log:function(text){
            var date = new Date(Date.now());
            console.log(date.toString("hh:mm:ss")+" - "+text);
        },
        checkQuiesced:function(){
            if(this.isQuiesced){
                $("#quiesceLink").text("Reactivate");
                $("span#quiescedLabel").show();
                $("#topNavbar").addClass("navbar-inverse");
            }
            else{
                $("#quiesceLink").text("Quiesce");
                $("span#quiescedLabel").hide();
                $("#topNavbar").removeClass("navbar-inverse");
            }
        }
    };
    _.bindAll(app,'setCookie','getCookie');
    return _.extend(Backbone.Events,app);
});