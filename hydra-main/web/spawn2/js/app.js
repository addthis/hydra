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
        loginSSLDefault:true,
        loginExpirationSeconds:86400,
        sudoExpirationSeconds:900,
        loginDialog:null,
        loginTimeoutId:null,
        sudoTimeoutId:null,
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
        authprefix:function() {
            var useSSL = app.loginSSLDefault || (window.location.href.lastIndexOf("https", 0) == 0);
            return (useSSL ? "https://" : "http://") + window.location.hostname + ":" +
                (useSSL ? "5053" : "5052");
        },
        initialize:function() {
            // delete legacy cookie
            Cookies.set("username", "", {expires:0, path:"/spawn2"});
            $.ajax({
                url: '/update/settings',
                dataType: 'json',
                success: function(response) {
                    app.loginSSLDefault = response.sslDefault;
                    app.loginExpirationSeconds = response.authTimeout;
                    app.sudoExpirationSeconds = response.sudoTimeout;
                    var username = Cookies.get("username");
                    var token = Cookies.get("token");
                    var tokenExpires = Cookies.get("tokenExpires");
                    if (username && token && tokenExpires) {
                        var delta = new Date(tokenExpires).getTime() - new Date().getTime();
                        if (delta > 0) {
                            app.loginTimeoutId = setTimeout(app.loginTimeout, delta);
                        }
                    }
                    if (window.location.href.search("login.html") == -1) {
                        if (!username || !token) {
                            window.location=app.authprefix() + "/spawn2/login.html";
                        } else {
                            $.ajax({
                                type: 'POST',
                                url: '/authentication/validate',
                                data: {
                                    user: username,
                                    token: token
                                },
                                dataType: 'text',
                                success: function(response) {
                                    if (response === "true") {
                                        app.user.set("username", username);
                                        app.user.set("token", token);
                                    } else {
                                        window.location=app.authprefix() + "/spawn2/login.html";
                                    }
                                },
                                error: function(error) {
                                    alertify.error("Failure on /authentication/validate", 0);
                                }
                            });
                        }
                    }
                },
                error: function(error) {
                    alertify.error("Failure on /update/settings", 0);
                }
            });
        },
        authenticate:function(evt) {
            evt.preventDefault();
            var usernameInput = $("#loginUsername")[0];
            var passwordInput =  $("#loginPassword")[0];
            var tokenName = $("#loginToken")[0].value;
            var urlPath;
            if (tokenName == "token") {
                urlPath = "/authentication/login";
            } else if (tokenName == "sudo") {
              urlPath = "/authentication/sudo";
            } else {
                alertify.error("Unknown token name " + tokenName);
                return;
            }
            var username = $.trim(usernameInput.value);
            var password = passwordInput.value;
            usernameInput.value = "";
            passwordInput.value = "";
            var loginUrl = app.authprefix() + urlPath;
            $.ajax({
                type: 'POST',
                url: loginUrl,
                data: {
                    user: username,
                    password: password
                },
                dataType: 'text',
                success: function(response) {
                    if (app.loginDialog) {
                        app.loginDialog.close();
                    }
                    var token = response;
                    if (!token) {
                        alertify.error("Authentication error");
                        if (tokenName == "sudo") {
                            $("#sudoCheckbox").prop("checked", false);
                        }
                    } else {
                        if (tokenName == "token") {
                            var loginTimeout = new Date(new Date().getTime() + app.loginExpirationSeconds * 1000);
                            Cookies.set("username", username, {expires: loginTimeout});
                            Cookies.set("token", token, {expires: loginTimeout});
                            Cookies.set("tokenExpires", loginTimeout, {expires: loginTimeout});
                            if (app.loginTimeoutId) {
                                clearTimeout(app.loginTimeoutId);
                            }
                            app.loginTimeoutId = setTimeout(app.loginTimeout, app.loginExpirationSeconds * 1000);
                            app.user.set("username", username);
                            app.user.set("token", token);
                        } else {
                            var sudoTimeout = new Date(new Date().getTime() + app.sudoExpirationSeconds * 1000);
                            Cookies.set("sudo", token, {expires: sudoTimeout});
                            app.user.set("sudo", token);
                            if (app.sudoTimeoutId) {
                                clearTimeout(app.sudoTimeoutId);
                            }
                            app.sudoTimeoutId = setTimeout(app.sudoTimeout, app.sudoExpirationSeconds * 1000);
                        }
                        if (window.location.href.search("login.html") > -1) {
                            window.location="http://" + window.location.hostname + ":5052/spawn2/index.html#jobs";
                        }
                    }
                },
                error: function(error) {
                    if (app.loginDialog) {
                        app.loginDialog.close();
                    }
                    alertify.error("Accept our <a target=\"_blank\" href=\"" + app.authprefix() + "\">https certificate</a>", 0);
                }
            });
        },
        loginTimeout:function() {
            app.user.set("username", "");
            app.user.set("token", "");
            alertify.alert("Your session has expired. Press the login button in the top-right corner.");
        },
        sudoTimeout:function() {
            app.user.set("sudo", "");
            $("#sudoCheckbox").prop("checked", false);
        },
        login:function() {
            document.activeElement.blur();
            $("#loginToken")[0].value = "token";
            app.loginDialog = alertify.minimalDialog($('#loginForm')[0]);
        },
        sudo:function() {
            var checked =  $("#sudoCheckbox").is(':checked');
            if (checked) {
                var username = Cookies.get("username");
                var token = Cookies.get("token");
                var sudoToken = Cookies.get("sudo");
                if (!username || !token) {
                    alertify.error("Please login first");
                } else if (sudoToken) {
                    app.user.set("sudo", sudoToken);
                } else {
                    $("#loginToken")[0].value = "sudo";
                    $("#loginUsername")[0].value = username;
                    app.loginDialog = alertify.minimalDialog($('#loginForm')[0]);
                }
            } else {
                app.user.set("sudo", "");
            }
        },
        logout:function() {
            document.activeElement.blur();
            var username = Cookies.get("username");
            var token = Cookies.get("token");
            if (username) {
                var logoutUrl = "/authentication/logout";
                $.ajax({
                    type: 'POST',
                    url: logoutUrl,
                    data: {
                        user: username,
                        token: token
                    },
                });
            }
            Cookies.set("username", "", {expires:0});
            // delete legacy cookie
            Cookies.set("username", "", {expires:0, path:"/spawn2"});
            Cookies.set("token", "", {expires:0});
            Cookies.set("tokenExpires", "", {expires:0});
            Cookies.set("sudo", "", {expires:0});
            app.user.set("username", "");
            app.user.set("token", "");
            app.user.set("sudo", "");
            clearTimeout(app.loginTimeoutId);
            clearTimeout(app.sudoTimeoutId);
            app.loginTimeoutId = null;
            app.sudoTimeoutId = null;
        },
        authQueryParameters:function(parameters) {
            var user = app.user.get("username");
            var token = app.user.get("token");
            var sudo = app.user.get("sudo");
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
            alertify.confirm( ((app.isQuiesced?"un":"")+"quiesce the cluster? (if you don't know what you're doing, hit cancel!)"), function (e) {
                var parameters = {}
                parameters["quiesce"] = app.isQuiesced ? "0" : "1";
                app.authQueryParameters(parameters);
                $.ajax({
                    url: "/system/quiesce",
                    type: "GET",
                    data: parameters
                }).done(function(data){
                    alertify.message("Cluster "+(data.quiesced=="1"?"quiesced":"reactivated")+" successfully.");
                    app.isQuiesced= !app.isQuiesced;
                    app.checkQuiesced();
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
            if(app.isQuiesced){
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