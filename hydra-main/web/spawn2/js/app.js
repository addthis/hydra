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
    "alertify",
    "modules/server",
    "jscookie",
    "jquery",
    "underscore",
    "backbone",
    "alertify"
],
function(
    Router,
    alertify,
    server,
    Cookies
){
    var app = {
        router: new Router(),
        currentView:null,
        mainSelector:"#main",
        user: new Backbone.Model({username:"",token:"",sudo:""}),
        server:server,
        activeModels:[],
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
        login:function() {
            var self = this;
            var username = Cookies.get("username");
            var token = Cookies.get("token");
            if (_.isUndefined(username) || _.isUndefined(token)) {
                var alert = alertify.prompt("Enter username:","",function(evt, str){
                    username = $.trim(str);
                    token = username;
                    Cookies.set("username", username, {expires:1});
                    Cookies.set("token", token, {expires:1});
                    self.user.set("username", username);
                    self.user.set("token", token);
                });
                $(alert.el).find("#alertify-text").focus();
            } else {
                self.user.set("username", username);
                self.user.set("token", token);
            }
        },
        logout:function() {
           var self = this;
           Cookies.set("username", "", {expires:0});
           Cookies.set("token", "", {expires:0});
           self.user.set("username", "");
           self.user.set("token", "");
        },
        authQueryParameters:function(parameters) {
            var self = this;
            var user = self.user.get("username");
            var token = self.user.get("token");
            var sudo = self.user.get("sudo");
            parameters["user"] = user;
            parameters["token"] = token;
            if (sudo) {
                parameters["sudo"] = sudo;
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
                url: "/system/healthcheck?details=true",
                type: "GET"
            }).done(function(data){
                if (data.everythingOK) {
                    alertify.message("Health check passed");
                } else {
                    alertify.alert("Health check failed: " + JSON.stringify(data));
                }
            });
        },
        isQuiesced:false,
        quiesce:function(){
            var self=this;
            alertify.confirm( ((this.isQuiesced?"un":"")+"quiesce the cluster? (if you don't know what you're doing, hit cancel!)"), function (e) {
                $.ajax({
                    url: "/system/quiesce",
                    type: "GET",
                    data: {quiesce:(self.isQuiesced?"0":"1")}
                }).done(function(data){
                    alertify.message("Cluster "+(data.quiesced=="1"?"quiesced":"reactivated")+" successfully.");
                    self.isQuiesced= !self.isQuiesced;
                    self.checkQuiesced();
                }).fail(function(){
                    alertify.alert("You do not have sufficient privileges to quiesce cluster");
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
    return _.extend(Backbone.Events,app);
});