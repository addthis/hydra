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
    Cookies,
    $,
    _
){
    var app = {
        router: new Router(),
        currentView:null,
        mainSelector:"#main",
        user: new Backbone.Model({username:"",token:"",sudo:""}),
        server:server,
        loginSSLDefault:true,
        loginDialog:null,
        jobDefaults: {},
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
            return (useSSL ? "https://" : "http://") + window.location.hostname + ":" + (useSSL ? "5053" : "5052");
        },
        initialize:function() {
            // delete legacy cookies
            Cookies.set("username", "", {expires:0, path:"/spawn2"});
            Cookies.set("spawn", "", {expires:0, path:"/spawn2"});
            $.ajax({
                url: '/job/defaults',
                dataType: 'json',
                success: function(response) {
                    app.jobDefaults = response;
                }
            });
            $.ajax({
                url: '/update/settings',
                dataType: 'json',
                success: function(response) {
                    var username = Cookies.get("username");
                    var token = Cookies.get("token");
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
                                app.user.set("sudo", Cookies.get("sudo"));
                            } else {
                                alertify.error("Invalid credentials. Please login. (Click to dismiss)", 0);
                            }
                        },
                        error: function(error) {
                            alertify.error("Failure on /authentication/validate", 0);
                        }
                    });
                },
                error: function(error) {
                    alertify.error("Failure on /update/settings", 0);
                }
            });
        },
        cbcontrol: function(hasAdminRight) {
            if(hasAdminRight === "true" || !hasAdminRight) {
                if(hasAdminRight === "true") {
                    Cookies.set("cansudo", "true");
                }
                if( Cookies.get("sudo")) {
                    app.sudoCheckbox(true, true);
                    return "checkSudo";
                } else if(Cookies.get("cansudo")) {
                    app.sudoCheckbox(false, true);
                    return "uncheckSudo";
                }
            } else if(hasAdminRight === "false") {
                app.sudoCheckbox(false, false);
                return "noSudo";
            }
            app.user.set("sudo", Cookies.get("sudo"));
        },
        // check if user belongs to adminGroup or adminUser or not
        isadmin: function(username, token, cbcontrol) {
            $.ajax({
                type: 'POST',
                url:"/authentication/isadmin",
                data: {
                    user: username,
                    token: token
                },
                dataType: 'Text',
                success: function(response) {
                    app.cbcontrol(response);
                },
                error: function(error) {
                    alertify.error("Failure on /authentication/isadmin", 0);
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
            } else {
                alertify.error("Unknown token name " + tokenName);
                return;
            }
            var username = $.trim(usernameInput.value);
            var password = passwordInput.value;
            var loginUrl = app.authprefix() + urlPath;
            usernameInput.value = "";
            passwordInput.value = "";
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
                    } else {
                        if (tokenName == "token") {
                            Cookies.set("username", username, {expires: 730});
                            Cookies.set("token", token, {expires: 730});
                            Cookies.set("sudo", "", {expires:0});
                            app.user.set("username", username);
                            app.user.set("token", token);
                            app.user.set("sudo", Cookies.get("sudo"));
                            app.isadmin(username, token);
                        }
                    }
                },
                error: function(jqXHR, textStatus) {
                    console.log("jqXHR = " + jqXHR + ", textStatus = " + textStatus);

                    if (app.loginDialog) {
                        app.loginDialog.close();
                    }
                    if (jqXHR.status === 401) {
                        alertify.error("Invalid username/password provided");
                    } else {
                        alertify.error("Accept our <a target=\"_blank\" href=\"" + app.authprefix() + "/spawn2/landing.html"+ "\">https certificate</a>", 0);
                    }
                }
            });
        },
        login:function() {
            document.activeElement.blur();
            $("#loginToken")[0].value = "token";
            app.loginDialog = alertify.minimalDialog($('#loginForm')[0]);
        },
        unsudo: function(username, token) {
            var unsudoUrl = "/authentication/unsudo";
            $.ajax({
                type: 'POST',
                url: unsudoUrl,
                data: {
                    username: username,
                    token: token,
                    sudo: Cookies.get("sudo")
                },
            });
            Cookies.set("sudo", "", {expires:0});
            app.user.set("sudo", Cookies.get("sudo"));
        },
        cansudo: function(username, token) {
            var checked =  $("#sudoCheckbox").is(':checked');
            if (checked) {
                if (!username || !token) {
                    alertify.error("Please login first");
                    return false;
                }
            } else {
                app.unsudo(username, token);
                app.user.set("sudo", Cookies.get("sudo"));
                return false;
            }
            return true;
        },
        setTimer: function (username, token, sudoToken, mins) {
            var inTime = new Date(new Date().getTime() + mins * 60 * 1000);
            Cookies.set("sudo", sudoToken, {expires: inTime});
            var timerId = setInterval(function() {
                app.unsudo(username, token);
                app.unsetTimer(timerId);
            }, mins*60*1000);
        },
        unsetTimer: function(timerId) {
            clearTimeout(timerId);
        },
        sudo:function() {
            var sudoUrl = app.authprefix() + "/authentication/sudo";
            var username = Cookies.get("username");
            var token = Cookies.get("token");
            if(!app.cansudo(username, token)) {
                return;
            }
            $.ajax({
                type: 'POST',
                url: sudoUrl,
                data: {
                    user: username,
                    token: token
                },
                dataType: 'text',
                success: function (response) {
                    var sudoToken = response;
                    if (!sudoToken) {
                        alertify.error("Authentication error");
                        sudoCheckbox(false, false);
                    } else {
                        var message = "Please confirm that you want to enable sudo privilege";
                        alertify.confirm(message, function(e) {
                            // BUG: swhen you refresh the page before expire, GUI does not uncheck automatically after expire
                            app.setTimer(username, token, sudoToken, 15);
                            app.user.set("sudo", sudoToken);
                            app.sudoCheckbox(true, true);
                        }, function(e) {
                            app.user.set("sudo", Cookies.get("sudo"));
                        });
                    }
                },
                error: function (jqXHR, textStatus) {
                    if (jqXHR.status === 401) {
                        alertify.error("Invalid username/token provided");
                    }
                }
            })
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
            Cookies.set("username", "", {expires:0, path:"/spawn2"}); // delete legacy cookie
            Cookies.set("spawn", "", {expires:0, path:"/spawn2"}); // delete legacy cookie
            Cookies.set("token", "", {expires:0});
            Cookies.set("tokenExpires", "", {expires:0});
            Cookies.set("sudo", "", {expires:0});
            Cookies.set("cansudo", "", {expires:0});
            app.user.set("username", "");
            app.user.set("token", "");
            app.user.set("sudo", "");
            app.sudoCheckbox(false, false);
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
            return parameters;
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
        },
        sudoCheckbox:function(checked, show){
            $("#sudoCheckbox").prop("checked", checked);

            if(show) {
                 $("#sudoCheckboxLabel").show();
                $("#sudoCheckbox").show();
            } else {
                $("#sudoCheckboxLabel").hide();
                $("#sudoCheckbox").hide();
            }
        }
    };
    return _.extend(app, Backbone.Events);
});
