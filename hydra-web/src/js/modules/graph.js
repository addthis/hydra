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
    "jquery",
    "underscore",
    "backbone",
    "d3"
],
function(
    $,
    _,
    Backbone,
    d3
){
    var TreeForceDirectedGraphView = Backbone.View.extend({
        initialize:function(){
            _.bindAll(this);
        },
        render:function(){
            var w = 960,
                h = 500,
                node,
                link,
                root;

            var force = d3.layout.force()
                .on("tick", this.tick)
                .size([w, h]);

            var vis = d3.select(this.$el.get(0)).append("svg")
                .attr("width", w)
                .attr("height", h);

            this.update();
            return this;
        },
        // Returns a list of all nodes under the root.
        flatten:function(root) {
            var nodes = [], i = 0;

            function visit(node) {
                if (node.children) node.children.forEach(visit);
                if (!node.id) node.id = ++i;
                nodes.push(node);
            }

            visit(root);
            return nodes;
        },
        // Toggle children on click.
        click: function(d) {
            if (d.children) {
                d._children = d.children;
                d.children = null;
            } else {
                d.children = d._children;
                d._children = null;
            }
            this.update();
        },
        // Color leaf nodes orange, and packages white or blue.
        color:function(d) {
            return d._children ? "#3182bd" : d.children ? "#c6dbef" : "#fd8d3c";
        },
        tick: function() {
            link.attr("x1", function(d) { return d.source.x; })
                .attr("y1", function(d) { return d.source.y; })
                .attr("x2", function(d) { return d.target.x; })
                .attr("y2", function(d) { return d.target.y; });

            node.attr("cx", function(d) { return d.x; })
                .attr("cy", function(d) { return d.y; });
        },
        update: function() {
            var w = 960,
                h = 500,
                node,
                link,
                root;

            var force = d3.layout.force()
                .on("tick", this.tick)
                .size([w, h]);

            var vis = d3.select(this.$el.get(0)).append("svg")
                .attr("width", w)
                .attr("height", h);
            var root = this.model.toJSON();
            var nodes = this.flatten(root),
                links = d3.layout.tree().links(nodes);

            // Restart the force layout.
            force.nodes(nodes)
                .links(links)
                .start();

            // Update the links…
            link = vis.selectAll("line.link")
                .data(links, function(d) { return d.target.id; });

            // Enter any new links.
            link.enter().insert("svg:line", ".node")
                .attr("class", "link")
                .attr("x1", function(d) { return d.source.x; })
                .attr("y1", function(d) { return d.source.y; })
                .attr("x2", function(d) { return d.target.x; })
                .attr("y2", function(d) { return d.target.y; });

            // Exit any old links.
            link.exit().remove();

            // Update the nodes…
            node = vis.selectAll("circle.node")
                .data(nodes, function(d) { return d.id; })
                .style("fill", this.color);

            node.append("text")
                .attr("dx", 12)
                .attr("dy", ".35em")
                .text(function(d) { return d.name });
            // Enter any new nodes.
            node.enter().append("svg:circle")
                .attr("class", "node")
                .attr("cx", function(d) { return d.x; })
                .attr("cy", function(d) { return d.y; })
                .attr("r", function(d) { return Math.sqrt(d.size) / 10 || 30; })
                .style("fill", this.color)
                .on("click", this.click)
                .call(force.drag);

            // Exit any old nodes.
            node.exit().remove();
        }
    });
    var ForceDirectedGraphView = Backbone.View.extend({
        initialize:function(){

        },
        render:function(){
            var width = 960,height = 500;
            var color = d3.scale.category20();
            var force = d3.layout.force()
                .charge(-120)
                .linkDistance(60)
                .size([width, height]);
            var svg = d3.select(this.$el.get(0)).append("svg")
                .attr("width", width)
                .attr("height", height);
            var graph = this.model.toJSON();
            force.nodes(graph.nodes)
                .links(graph.links)
                .start();
            var link = svg.selectAll(".link")
                .data(graph.links)
                .enter().append("line")
                .attr("class", "link")
                .style("stroke-width", function(d) { return Math.sqrt(d.value); });
            var node = svg.selectAll(".node")
                .data(graph.nodes)
                .enter().append("circle")
                .attr("class", "node")
                .attr("r", 10)
                .style("fill", function(d) { return color(d.group); })
                .call(force.drag);
            node.append("title")
                .text(function(d) { return d.name; });
            node.append("text")
                .text(function(d) { return d.name; });
            force.on("tick", function() {
                link.attr("x1", function(d) { return d.source.x; })
                    .attr("y1", function(d) { return d.source.y; })
                    .attr("x2", function(d) { return d.target.x; })
                    .attr("y2", function(d) { return d.target.y; });
                node.attr("cx", function(d) { return d.x; })
                    .attr("cy", function(d) { return d.y; });
            });
            return this;
        }
    });
    var TreeDependencyGraphView = Backbone.View.extend({
        initialize:function(){
        },
        render:function(){
            var diameter = 760;
            var tree = d3.layout.tree()
                .size([360, diameter / 2 - 120])
                .separation(function(a, b) {
                    return (a.parent == b.parent ? 1 : 2) / a.depth;
                });
            var diagonal = d3.svg.diagonal.radial()
                .projection(function(d) {
                    var x = (d.x/180.0)*Math.PI;
                    return [d.y, (d.x / 180) * Math.PI];
                });
            var svg = d3.select(this.$el.get(0)).append("svg")
                .attr("width", diameter)
                .attr("height", diameter - 150)
                .append("g")
                .attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
            var nodes = tree.nodes(this.model.toJSON());
            var links = tree.links(nodes);
            var link = svg.selectAll(".link")
                .data(links)
                .enter().append("path")
                .attr("class", "link")
                .attr("d", diagonal);
            var node = svg.selectAll(".node")
                .data(nodes)
                .enter().append("g")
                .attr("class", "node")
                .attr("transform", function(d) {
                    return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")";
                });
            node.append("circle")
                .attr("r", 4.5)
                .attr("style", function(d) {
                    return "stroke: " + d.color;
                });
            var anchor = node.append("a")
                .attr("xlink:href", function(d) {
                    return "#jobs/" + d.jobId + "/conf"
                });
            anchor.append("text")
                .attr("dy", ".31em")
                .attr("text-anchor", function(d) {
                    return "start";
                })
                .attr("transform", function(d) {
                    return "rotate(" + (90 - d.x) + ")translate(8)"
                })
                .text(function(d) {
                    return d.name;
                });
            return this;
        }
    });
    var ForceDirectedGraphModel = Backbone.Model.extend({
        initialize:function(opts){
            _.bindAll(this);
            this.jobId=opts.jobId;
        },
        url:function(){
            return "/job/dependencies/sinks?id="+this.jobId;
        },
        parse:function(data){
            var nodeMap = {}, nodeList=[];
            _.each(data.nodes,function(node,idx){
                nodeMap[node.id]=nodeList.length;
                nodeList.push({
                    name:node.id,
                    group:1
                });
            });
            var edges = [];
            _.each(data.edges,function(edge){
                edges.push({
                    source:nodeMap[edge.source],
                    target:nodeMap[edge.sink],
                    value:1
                });
            });
            return {
                nodes:nodeList,
                links:edges
            };
        }
    });
    var TreeGraphModel = Backbone.Model.extend({
        initialize:function(opts){
            _.bindAll(this,'url','parse','buildGraph');
            this.jobId=opts.jobId;
        },
        url:function(){
            return "/job/dependencies/connected?id="+this.jobId;
        },
        parse:function(data){
            var nodes = {};
            _.each(data.nodes,function(node){
                nodes[node.id]=node;
            });
            var edges = {};
            _.each(data.edges,function(edge){
                if(_.isUndefined(edges[edge.source])){
                    edges[edge.source]=[edge.sink];
                }
                else{
                    edges[edge.source].push(edge.sink);
                }
            });
            var graph = this.buildGraph(nodes,edges);
            return graph;
        },
        buildGraph:function(nodes, edges){
            // poor man's topological sort
            var result = _.reduce(
                _.flatten(_.values(edges)),
                function(nodeList, child){
                    return _.without(nodeList, child);
                },
                _.keys(nodes)
            );
            var root = _.first(result);

            return this.buildGraphHelp(root, nodes, edges);
        },
        buildGraphHelp:function(nodeId,nodes,edges){
            var childrenIds = edges[nodeId];
            var childrenNodes = [],self=this;
            _.each(childrenIds,function(child){
                var childNode = self.buildGraphHelp(child,nodes,edges);
                childrenNodes.push(childNode);
            });
            return {
                name:nodeId.substring(0,10),
                jobId:nodeId,
                children: childrenNodes,
                color: nodeId === this.jobId ? "green" : "blue",
                size:1
            };
        }
    });
    return {
        TreeForceDirectedGraphView:TreeForceDirectedGraphView,
        ForceDirectedGraphView:ForceDirectedGraphView,
        ForceDirectedGraphModel:ForceDirectedGraphModel,
        TreeGraphModel:TreeGraphModel,
        TreeDependencyGraphView:TreeDependencyGraphView
    };
});