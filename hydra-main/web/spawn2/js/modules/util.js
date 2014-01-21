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
define([],function(){
    var util = {
        contains:function(self,str){
			return self.indexOf(str) >= 0;
        },
        convertToDFH: function(bytes){
            if(typeof bytes=="number" && bytes>=0){
                var units = ["B","K","M","G","T"];
                var shorterSize = bytes;
                for(var u=0;u<units.length && Math.floor(shorterSize/10)>10;u++){
                    shorterSize=(shorterSize/1024).toFixed(1);
                }
                return shorterSize+" "+units[u];
            }
            else{
                return "-";
            }
        },
        shortenNumber:function(num){
            if(typeof num=="number"){
                if(num===0){
                    return 0;
                }
                else if(num>1 || num<=-1){
                    var units = ["","k","M"];
                    var shorterSize = num.toFixed(2);
                    for(var u=0;u<units.length && shorterSize>1000;u++){
                        shorterSize=(shorterSize/1000).toFixed(2);
                    }
                    return shorterSize+""+units[u];
                }
                else if(num<=0.01 && num>-0.01){
                    var shorterSize = num.toExponential(1);
                    return shorterSize;
                }
                else if(num<=1 && num>-1){
                    var shorterSize = num.toFixed(2);
                    return shorterSize;
                }
                else{
                    return "-";
                }
            }
            else{
                return "-";
            }
        },
        convertToDateTimeText: function(time,format){
            format= format || "MM/dd/yy HH:mm";
            if(time && time>0)
                return new Date(time).toString(format);
            else
                return "-";
        },
        fromCamelToTitleCase:function(str){
            if (str == null || str == "") {
                return str;
            }
            var newText = "";
            var characters = str.split("");
            for (var i = 0; i < characters.length; i++) {
                if (characters[i] == characters[i].toUpperCase()
                    && i != 0
                    && !(characters[i + 1] == characters[i + 1].toUpperCase())
                    && characters[i - 1] != " ") {
                    newText += " ";
                }
                newText += characters[i];
             }
            return newText;
        },
        startsWith:function(self,str){
			return self.indexOf(str) == 0;
        },
        shorten:function(word,len){
			return (word.length>len? word.substr(0,len)+"...": word);
        },
        formatPercent:function(v){
			if (v) {
				return Math.round(100 * v);
			}
			return '';        
        },
        statusTextForExitCode:function(code){
            if (code == null) {
                return "";
            }
            if (code > 0) {
                return "process exit: " + code;
            }
            else {
                switch(code) {
                case -100:
                    return "backup failed";
                case -101:
                    return "replicate failed";
                case -102:
                    return "revert failed";
                case -103:
                    return "swap failed";
                case -104:
                    return "failed host";
                case -105:
                    return "kick failed";
                case -106:
                    return "dir error";
                case -107:
                    return "script exec error";
                default:
                    return "";
                }
            }
        },
    	alertTypes: {0: "On Job Error", 1: "On Job Completion", 2: "Runtime Exceeded", 3: "Rekick Timeout", 4: "Split Canary", 5: "Map Canary"},

    };
    return util;
});
