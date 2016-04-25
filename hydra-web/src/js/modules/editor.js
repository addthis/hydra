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
	"underscore",
	"ace",
	"brace/theme/monokai",
	"brace/mode/javascript",
	"brace/ext/searchbox",
	"backbone"
],
function(
	_,
    ace
){
    var Editor={};
    Editor.AceView = Backbone.View.extend({
		events:{
			"change":"handleEditorChange"
		},
		initialize:function(options){
			_.bindAll(this,'handleEditorChange');
            options = options || {};
            this.keyName=options.keyName;
			this.readOnly=(options.readOnly?options.readOnly:false);
			this.hasRendered=false;
			this.hasReset=false;
			this.scrollTo=options.scrollTo || {line: 0, col: 0},
			this.listenTo(this.model,"change:"+options.keyName,this.render);
			//this.listenTo(this.model,"reset",this.handleReset);
			this.model.bind("reset",this.handleReset);
            if(!_.isUndefined(options.css)){
                this.$el.css(options.css);
            }
		},
		handleEditorChange:function(event){
			this.model.set(this.keyName, this.views.editor.session.getValue(),{silent:true});
			if(this.hasReset){
				this.model.trigger("user_edit");
			}
            else{
                this.views.editor.resize();
            }
		},
		handleReset:function(event){
			this.views.editor.session.setValue(this.model.get(this.keyName));
			this.hasReset=true;
		},
		render:function(){
			this.views={
				editor: ace.edit(this.el)
			};

			if(this.readOnly){
				this.views.editor.setReadOnly(this.readOnly);
			}
			this.views.editor.setShowPrintMargin(false);
            this.views.editor.getSession().setUseWorker(false);
            this.views.editor.getSession().setOption('useSoftTabs',true);
			this.views.editor.getSession().setMode("ace/mode/javascript");
			this.views.editor.setTheme("ace/theme/monokai");
			this.views.editor.getSession().on("change",this.handleEditorChange);
			this.views.editor.session.setValue(this.model.get(this.keyName) || '');
			this.hasRendered=true;
            this.views.editor.resize();
			this.views.editor.scrollToLine(this.scrollTo.line, true, false);
			this.views.editor.gotoLine(this.scrollTo.line, this.scrollTo.col);
			return this;
		},
		handleChange:function(event){
			this.views.editor.session.setValue(this.model.get(this.keyName));
		},
		close:function(){
			this.stopListening();
			this.$el.remove();
			return this;
		}
    });
    return Editor;
});
