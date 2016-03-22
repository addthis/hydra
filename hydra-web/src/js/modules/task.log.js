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
    'backbone'
],
function(
){
    var Model = Backbone.Model.extend({
        initialize:function(options){
            options=options || {};
            this.node = options.node;
            this.jobUuid=options.jobUuid;
            this.lines=options.lines;
            this.runsAgo=options.runsAgo;
            this.host=options.host;
            this.port=options.port;
            this.stdout=options.stdout;
            this.stdout=undefined;
        },
        url:function(){
            var url= "/job/" + this.jobUuid + "/log?out="+ (this.stdout ? "1" : "0") +
            "&lines=" + this.lines + "&node=" + this.node + "&runsAgo=" + this.runsAgo +
            "&minion=" + this.host + "&port=" + this.port;

            if(!_.isUndefined(this.offset)){
                url+="&offset="+this.offset;
            }
            return url;
        },
        sync: function(method, model, options){
            options.dataType = "json";
            return Backbone.sync(method, model, options);
        },
        parse:function(data){
            if(_.isUndefined(data.out)){
                data.out="Log file not found.";
            } else if (data.offset === 0) {
                data.out="Log file is empty.";
            }
            return data;
        },
        clear:function(){
            this.set({
                lastModified:undefined,
                lines:undefined,
                node:undefined,
                offset:undefined,
                out:"",
                runsAgo:undefined
            },{silent:true});
            this.trigger("clear");
        }
    });
    return {
        Model:Model
    };
});