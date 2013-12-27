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
define(["underscore","backbone"],function(_,Backbone){
    var server= _.extend({
        connect:function(){
            _.bindAll(this,'sendText','sendJSON','handleOpen','handleClose','handleMessage','stopHeartbeat','startHeartbeat');
            this.ws=new WebSocket("ws://"+window.location.host+"/ws?user="+this.user);//this.user.get("username"));
            this.ws.onopen=this.handleOpen;
            this.ws.onclose=this.handleClose;
            this.ws.onmessage=this.handleMessage;
            this.heartbeat=undefined;
            return this;
        },
        user:"anonymous",
        sendText:function(text){
            this.ws.send(text);
            return this;
        },
        sendJSON: function(data){
            var stringified = JSON.stringify(data);
            this.ws.send(stringified);
            return this;
        },
        handleOpen:function(event){
            console.log("Connection with WebSocketManager has been established.");
            this.startHeartbeat();
        },
        handleClose:function(event){
            console.log("Closing connection... code: "+event.code+", reason: "+event.reason+", wasClean: "+event.wasClean);
            this.stopHeartbeat();
        },
        handleMessage:function(event){
            if(!_.isEqual(event.data,"pong")){
                var data = JSON.parse(event.data);
                var message = JSON.parse(data.message);
                if(_.isEqual(data.topic,"event.batch.update")){
                    var count = {},self=this;
                    _.each(message,function(ev){
                        self.trigger(ev.topic,ev.message);
                    });
                }
                else{
                    this.trigger(data.topic,message);
                }
            }
        },
        stopHeartbeat:function(){
            if(!_.isUndefined(this.heartbeat)){
                clearInterval(this.heartbeat);
                this.heartbeat=undefined;
            }
        },
        startHeartbeat:function(){
            if(_.isUndefined(this.heartbeat)){
                var self=this;
                this.heartbeat = setInterval(function(){
                    self.sendText("ping");
                },10000);
            }
        }
    }, Backbone.Events);
    return server;
});
