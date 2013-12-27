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
            this.host=options.host;
            this.port=options.port;
            this.stdout=options.stdout;
            this.stdout=undefined;
        },
        url:function(){
            var url= "http://"+this.host+":"+this.port+"/job.log?out="+(this.stdout?'1':'0')+"&id="+this.jobUuid+"&lines="+this.lines+"&node="+this.node;
            if(!_.isUndefined(this.offset)){
                url+="&offset="+this.offset;
            }
            return url;
        },
        sync: function(method, model, options){
            options.dataType = "jsonp";
            return Backbone.sync(method, model, options);
        },
        clear:function(){
            this.set({
                lastModified:undefined,
                lines:undefined,
                node:undefined,
                offset:undefined,
                out:""
            },{silent:true});
            this.trigger("clear");
        }
    });
    return {
        Model:Model
    };
});