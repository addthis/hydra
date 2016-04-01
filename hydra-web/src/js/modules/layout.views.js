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
    "backbone"
],
function(
    Backbone
){
    var Layout={};
    Layout.VerticalSplit = Backbone.View.extend({
        initialize:function(options){
            options=options || {};
            this.leftView = options.leftView || new Backbone.View;
            this.rightView = options.rightView || new Backbone.View;
            this.leftWidth = options.leftWidth || 50;
            this.rightWidth = options.rightWidth || 50;
            this.views={};
        },
        render:function(){
            this.$el.empty();
            this.views.left=new Backbone.View;
            this.views.right=new Backbone.View;
            //top view
            this.views.left.$el.addClass("left-view");
            this.views.left.$el.css({
                "width":this.leftWidth+"%"
            });
            this.views.left.$el.append(this.leftView.$el);
            this.$el.append(this.views.left.$el);
            this.leftView.render();
            //bottom view
            this.views.right.$el.addClass("right-view");
            this.views.right.$el.css({
                "width":this.rightWidth+"%"
            });
            this.views.right.$el.append(this.rightView.$el);
            this.$el.append(this.views.right.$el);
            this.rightView.render();
            return this;
        },
        remove:function(){
            this.leftView.remove();
            this.rightView.remove();
            Backbone.View.prototype.remove.apply(this,[]);
        }
    });
    Layout.HorizontalSplit = Backbone.View.extend({
        initialize:function(options){
            options=options || {};
            this.bottomView = options.bottomView || new Backbone.View;
            this.topView = options.topView || new Backbone.View;
            this.bottomHeight = options.bottomHeight || "50%";
            this.topHeight = options.topHeight || "50%";
            this.views={};
        },
        render:function(){
            this.$el.empty();
            this.views.top=new Backbone.View;
            this.views.bottom=new Backbone.View;
            //top view
            this.views.top.$el.addClass("top-view");
            this.views.top.$el.css({
                "height":this.topHeight+'%'
            });
            this.views.top.$el.append(this.topView.$el);
            this.$el.append(this.views.top.$el);
            this.topView.render();
            //bottom view
            this.views.bottom.$el.addClass("bottom-view");
            this.views.bottom.$el.css({
                "height":this.bottomHeight+'%'
            });
            this.views.bottom.$el.append(this.bottomView.$el);
            this.$el.append(this.views.bottom.$el);
            this.bottomView.render();
            return this;
        },
        remove:function(){
            this.topView.remove();
            this.bottomView.remove();
            Backbone.View.prototype.remove.apply(this,[]);
        }
    })
    Layout.HorizontalDividedSplit = Backbone.View.extend({
        initialize:function(options){
            options=options || {};
            this.bottomView = options.bottomView || new Backbone.View;
            this.topView = options.topView || new Backbone.View;
            this.dividerView = options.dividerView || new Backbone.View;
            this.bottomHeight = options.bottomHeight || 49;
            this.dividerHeight = options.dividerHeight || 2;
            this.topHeight = options.topHeight || 49;
            this.views={};
        },
        render:function(){
            this.$el.empty();
            this.views.top=new Backbone.View;
            this.views.divider=new Backbone.View;
            this.views.bottom=new Backbone.View;
            //top view
            this.views.top.$el.addClass("top-view");
            this.views.top.$el.css({
                "height":this.topHeight+"%"
            });
            this.views.top.$el.append(this.topView.$el);
            this.$el.append(this.views.top.$el);
            this.topView.render();
            //divider view
            this.views.divider.$el.addClass("divider-view");
            this.views.divider.$el.css({
                "height":this.dividerHeight+"%",
                "bottom":this.bottomHeight+"%",
                "top":this.topHeight+"%"
            });
            this.views.divider.$el.append(this.dividerView.$el);
            this.$el.append(this.views.divider.$el);
            this.dividerView.render();
            //bottom view
            this.views.bottom.$el.addClass("bottom-view");
            this.views.bottom.$el.css({
                "height":this.bottomHeight+"%"
            });
            this.views.bottom.$el.append(this.bottomView.$el);
            this.$el.append(this.views.bottom.$el);
            this.bottomView.render();
            return this;
        },
        remove:function(){
            this.topView.remove();
            this.dividerView.remove();
            this.bottomView.remove();
            Backbone.View.prototype.remove.apply(this,[]);
        }
    });;
    Layout.ClosableView = Backbone.View.extend({
        initialize:function(options){
            options = options || {};
            this.views={
                main: options.main || new Backbone.View,
                top: options.top || new Backbone.View
            };
        },
        render:function(){
            this.views.top.$el.addClass("top-view");
            this.views.main.$el.addClass("main-view");
            this.$el.append(this.views.top.$el);
            this.$el.append(this.views.main.$el);
            this.views.top.render();
            this.views.main.render();
            return this;
        }
    });
    return Layout;
});