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

({
    name:"main"
    ,out:"../../js/main.min.js"
    ,preserveLicenseComments:false
    ,include: "requireLib"
    ,excludeShallow:[
        "json!setupData"
    ]
    ,baseUrl:"../"
    ,paths: {
        "requireLib": "../js/vendor/require"
        ,"jquery": "../js/vendor/jquery-1.9.1"
        ,"backbone": "../js/vendor/backbone"
        ,"underscore": "../js/vendor/underscore"
        ,"bootstrap":"../js/vendor/bootstrap"
        ,"jquery.dataTable":"../js/vendor/jquery.dataTables.nightly"
        ,"dataTable.scroller":"../js/vendor/dataTables.scroller"
        ,"domReady":"../js/vendor/domReady"
        //,"jquery.resize":"../js/vendor/jquery.ba-resize"
        ,"date":"../js/vendor/date"
        ,"setupData":"../data/setup.json"
        ,"d3":"../js/vendor/d3.v2"
        ,"nvd3":"../js/vendor/js/nv.d3"
        ,"json":"../js/vendor/json"
        ,"text":"../js/vendor/text"
        ,"localstorage":"../js/vendor/backbone.localStorage"
        ,"jquery.cookie":"../js/vendor/jquery.cookie"
        ,"alertify":"../js/vendor/alertify"
        ,"ace":"../js/vendor/ace"
        ,"js-mode":"../js/vendor/mode-javascript"
        ,"ace/ext/searchbox":"../js/vendor/ace/ext/searchbox"
    }
    //,optimize:"none"
    ,shim: {
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
        "jquery":{
        exports:'jQuery'
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
        //"jquery.resize":['jquery'],
        'jquery.cookie':['jquery'],
        "dataTable.scroller":['jquery.dataTable']
    },
    packages: [
        {
            name: 'ace',
            location: '../js/vendor/ace/ace',
            main: 'ace'
        }
    ]
})
