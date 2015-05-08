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

require.config({
    baseUrl: "js",
    paths: {
        "jquery": "./vendor/jquery-1.9.1"
        ,"backbone": "./vendor/backbone"
        ,"jscookie": "./vendor/js.cookie"
        ,"underscore": "./vendor/underscore"
        ,"bootstrap":"./vendor/bootstrap"
        ,"jquery.dataTable":"./vendor/jquery.dataTables.nightly"
        ,"dataTable.scroller":"./vendor/dataTables.scroller"
        ,"domReady":"./vendor/domReady"
        ,"date":"./vendor/date"
        ,"setupData":"/update/setup"
        ,"d3":"./vendor/d3.v2"
        ,"nvd3":"./vendor/js/nv.d3"
        ,"json":"./vendor/json"
        ,"text":"./vendor/text"
        ,"localstorage":"./vendor/backbone.localStorage"
        ,"alertify":"./vendor/alertify"
        ,"ace":"./vendor/ace"
        ,"js-mode":"./vendor/mode-javascript"
        ,"js-worker":"./vendor/ace/worker/worker"
        ,"ace/ext/searchbox":"./vendor/ace/ext/searchbox"
        ,"git.template":"../../templates/git.properties.html"
    },
    shim: {
        'backbone': {
            deps: ['underscore', 'jquery'],
            exports: 'Backbone'
        },
        'underscore': {
            exports: '_'
        },
        'localstorage':{
            deps:['backbone']
        },
        'bootstrap':{
            deps:['jquery']
        },
        "jscookie":{
            exports:['Cookies']
        },
        "jquery":{
            exports:'jQuery'
        },
        "bootstrap":{
            deps:["jquery"]
        },
        "d3":{
            deps:[],
            exports:"d3"
        },
        "nvd3":{
            deps:["d3"],
            exports:"nv"
        },
        "jquery.dataTable":['jquery'],
        'jquery.cookie':['jquery'],
        "dataTable.scroller":['jquery.dataTable']
    },
    packages: [
        {
            name: 'ace',
            location: './vendor/ace/ace',
            main: 'ace'
        }
    ]
});
require([
    "app",
    "alertify",
    "jscookie",
    "json!setupData",
    "domReady",
    "backbone",
    "underscore",
    "jquery",
    "bootstrap",
    "date"
],
function(
    app,
    alertify,
    Cookies,
    setupData,
    domReady,
    Backbone,
    _,
    $
){
    alertify.defaults.glossary.title="";
    alertify.defaults.transition = "slide";
    alertify.defaults.theme.ok = "btn btn-primary";
    alertify.defaults.theme.cancel = "btn btn-danger";
    alertify.defaults.theme.input = "form-control";
    alertify.minimalDialog || alertify.dialog('minimalDialog',function(){
        return {
            main:function(content){
                this.setContent(content);
            }
        };
    });
    app.initialize();
    $('#loginForm').on('submit', app.authenticate);
});
