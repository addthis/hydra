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

function SVGGraph() {
	this.svgMain;
	var def;
	var svgObj;
	var svgns   = "http://www.w3.org/2000/svg";
	var xlinkns = "http://www.w3.org/1999/xlink";
	var viewer;
	var debugText;
	var viewerBox;
	var translateX = 0;
	var translateY = 0;
	var scale = 1;
	var maxScaleOnReset = 1.2;
	var minScaleOnReset = 0.6;
	var debugLabel;
	var maxZoom = 2.5;
	var minZoom = 0.2;
	var minX = 100000000;
	var minY = 100000000;
	var maxX = -100000000;
	var maxY = -100000000;
	
	var nodeMap = {};
	var nodeSelection = new Array();
	var edgeList = new Array();

	var divider;
	var nodeEntered;
	
	var nodeTypeToColor = {};
	var edgeTypeToColor = {};
	
	this.mapNodeTypeToColor = function(type, color) {
		nodeTypeToColor[type] = color;
	}
	
	this.mapEdgeTypeToColor = function(type, color) {
		edgeTypeToColor[type] = color;
	}
	
	this.setEnabledNode = function(node, enable) {
		node['enabled'] = enable;
		if (enable) {
			this.removeClass(node, 'disabled');
			//node.setAttributeNS(null, "opacity", 1.0);
		}
		else {
			this.addClass(node, 'disabled');
			//node.setAttributeNS(null, "opacity", 0.5);
		}
	}
	
	this.setEnabled = function(enable) {
		for (var i = 0; i < nodeSelection.length; ++i) {
			var selectedNode = nodeSelection[i];
			selectedNode['enabled'] = enable;
			if (enable) {
				this.removeClass(selectedNode, 'disabled');
			}
			else {
				this.addClass(selectedNode, 'disabled');
			}
		}
	}
	
	this.enableAll = function() {
		for (var key in nodeMap) {
			var node = nodeMap[key];
			this.setEnabledNode(node, true);
		}
	}
	
	var recurseId = 0;
	this.disableAllAncestors = function() {
		recurseId++;
		for (var i = 0; i < nodeSelection.length; ++i) {
			var selectedNode = nodeSelection[i];
			this.disableRecursively(selectedNode, recurseId);
		}
	}
	
	// Slow but don't expect it to be done often
	this.disableRecursively = function(node, recurseID) {
		// Visited this node, so don't visit it again.
		if (node['recurseID'] == recurseID) {
			return;
		}

		node['recurseID'] = recurseID;
		var children = new Array();
		for (var i = 0; i < edgeList.length; i++) {
			var edge = edgeList[i];
			if (node.id == edge.toNode) {
				var dependency = edge.fromNode;
				var dependent = edge.toNode;
				this.disableRecursively(nodeMap[dependency], recurseID);
			}
		}
	
		this.setEnabledNode(node, false);
	}
	
	// if the item succeeded, then make sure all of its parents are aslo disabled.
	this.initializeDisabled = function() {
		recurseId++;
		for (var key in nodeMap) {
			var node = nodeMap[key];
			if (node['type'] == 'selected') {
				this.disableRecursively(node, recurseID);
			}
		}
	}
	
	this.attachGraph = function(svgElement) {
		this.svgMain = svgElement;
		
		var post = this.svgMain.getElementsByTagName('g')[0];
		if (post) {
			this.svgMain.insertBefore(viewer, post);
		}
		else {
			this.svgMain.appendChild(viewer);
		}
		
		if (def) {
			this.svgMain.appendChild(def);
		}
		
		debugText = document.createElementNS(svgns, 'text');
		debugText.setAttributeNS(null, "class", "nodeDesc");
		debugText.setAttributeNS(null, "y", svgElement.parentNode.clientHeight - 15);
		debugText.setAttributeNS(null, "x", 15);
		debugText.setAttributeNS(null, "fill", "#000000");
		debugText.setAttributeNS(null, "font-size", 20);
		debugText.setAttributeNS(null, "font-family", "helvetica, arial, sans-serif");
		debugLabel = document.createTextNode("");
		var test = debugLabel;
		debugText.appendChild(debugLabel);
		this.svgMain.appendChild(debugText);
	}
	
	this.resetTransform = function(width, height) {
		var graphWidth = maxX - minX;
		var graphHeight = maxY - minY;

		scale = (height/graphHeight)*0.6;
		if (scale > maxScaleOnReset) {
			scale = maxScaleOnReset;
		}
		else if (scale < minScaleOnReset) {
			scale = minScaleOnReset;
		}

		translateX = (width - graphWidth*scale)/2 - minX*scale;
		translateY = minY + 30;

		//FIXME: This is distorting the image, it's not scaling it like it should
		viewer.setAttributeNS(null, "transform", "translate(" + translateX + "," + translateY + ") scale(" + scale + ")");
	}
	
	this.getScale = function() {
		return scale;
	}
	
	this.dettachGraph = function() {
		svgMain.removeChild(viewer);
		svgMain.removeChild(debugText);
	}
	
	this.initGraph = function() {
		viewer = document.createElementNS(svgns, 'g');
		svgObj = viewer;
		
		if (document.getElementById('buttonDefs') == null) {
			def = document.createElementNS(svgns, 'defs');
			def.setAttributeNS(null, "id", "buttonDefs");

			// Creating gradient for button shading
			var linearGradient = document.createElementNS(svgns, 'linearGradient');
			linearGradient.setAttributeNS(null, "id", "buttonGradient");
			linearGradient.setAttributeNS(null, "gradientUnits", "objectBoundingBox");
			linearGradient.setAttributeNS(null, "x1", "1");
			linearGradient.setAttributeNS(null, "x2", "1");
			linearGradient.setAttributeNS(null, "y1", "1.2");
			linearGradient.setAttributeNS(null, "y2", "0");
			
			var stop1 = document.createElementNS(svgns, 'stop');
			stop1.setAttributeNS(null, "style", "stop-color:#d9d9d9;stop-opacity:1;");
			stop1.setAttributeNS(null, "offset", "0");
			var stop2 = document.createElementNS(svgns, 'stop');
			stop2.setAttributeNS(null, "style", "stop-color:#d9d9d9;stop-opacity:0");
			stop2.setAttributeNS(null, "offset", "1");
			linearGradient.appendChild(stop1);
			linearGradient.appendChild(stop2);
			def.appendChild(linearGradient);

			var arrowHeadMarker = document.createElementNS(svgns, 'marker');
			arrowHeadMarker.setAttribute("id", "triangle");
			arrowHeadMarker.setAttribute("viewBox", "0 0 10 10");
			arrowHeadMarker.setAttribute("refX", "5");
			arrowHeadMarker.setAttribute("refY", "5");
			arrowHeadMarker.setAttribute("markerUnits", "strokeWidth");
			arrowHeadMarker.setAttribute("markerWidth", "4");
			arrowHeadMarker.setAttribute("markerHeight", "3");
			arrowHeadMarker.setAttribute("orient", "auto");
			var path = document.createElementNS(svgns, 'polyline');
			arrowHeadMarker.appendChild(path);
			path.setAttribute("points", "0,0 10,5 0,10 1,5");
			
			def.appendChild(arrowHeadMarker);
			
		}
		divider = document.createElement('_divider');
		viewer.appendChild(divider);
	};
	
	function intersectLineWithBox(x1, y1, x2, y2, node) {
		var boundingRect = node.getElementsByTagName('rect')[0];
		var width = parseFloat(boundingRect.getAttribute("width")) + 3;
		var height = parseFloat(boundingRect.getAttribute("height")) + 3;
		
		// Intersection to point to the boxes edge to place the arrowhead
		var deltaX = x1 - x2;
		var deltaY = y1 - y2;
		
		var boxm = height/width;
		var newX2 = x2;
		var newY2 = y2;
		var halfwidth = width/2;
		var halfheight = height/2;
		
		if (deltaY == 0) {
			if (deltaX > 0) {
				newY2 = y2;
				newX2 = x2 + halfwidth;
			}
			else {
				newY2 = y2;
				newX2 = x2 - halfwidth;
			}
		}
		else if (deltaX == 0) {
			if (deltaY > 0) {
				newY2 = y2 + halfheight;
				newX2 = x2;
			}
			else {
				newY2 = y2 - halfheight;
				newX2 = x2;
			}
		}
		else if (deltaX > 0) {
			var m = deltaY/deltaX;
			var inversem = deltaX/deltaY;
			if (m > boxm) {
				newY2 = y2 + halfheight;
				newX2 = x2 + inversem*halfheight;
			}
			else if (m < -boxm) {
				newY2 = y2 - halfheight;
				newX2 = x2 - inversem*halfheight;
			}
			else {
				newX2 = x2 + halfwidth;
				newY2 = y2 + m*halfwidth;
			}
		}
		else {
			var m = deltaY/deltaX;
			var inversem = deltaX/deltaY;
			if (m > boxm) {
				newY2 = y2 - halfheight;
				newX2 = x2 - inversem*halfheight;
			}
			else if (m < -boxm) {
				newY2 = y2 + halfheight;
				newX2 = x2 + inversem*halfheight;
			}
			else {
				newX2 = x2 - halfwidth;
				newY2 = y2 - m*halfwidth;
			}
		}
		
		var point = {};
		point['x'] = newX2;
		point['y'] = newY2;
		return point;
	}
	
	function setEdgePoints(edge) {
		var start = nodeMap[edge.fromNode];
		var end = nodeMap[edge.toNode];
		
		var x1 = parseFloat(start.getAttribute("gx")) + 1;
		var y1 = parseFloat(start.getAttribute("gy")) + 1;
		var x2 = parseFloat(end.getAttribute("gx")) + 1;
		var y2 = parseFloat(end.getAttribute("gy")) + 1;
		
		var point1 = intersectLineWithBox(x2, y2, x1, y1, start);
		var point2 = intersectLineWithBox(x1, y1, x2, y2, end);

		edge.setAttributeNS(null, "x1", point1.x);
		edge.setAttributeNS(null, "y1", point1.y);
		
		edge.setAttributeNS(null, "x2", point2.x);
		edge.setAttributeNS(null, "y2", point2.y);
	}
	
	this.createEdge = function(startNode, endNode, type) {
		var color = "#000000";
		if (edgeTypeToColor[type]) {
			color = edgeTypeToColor[type];
		}
		
		var g = document.createElementNS(svgns, 'g');
		var line = document.createElementNS(svgns, 'line');
		g.appendChild(line);
		g["type"] = type;
		g.setAttribute("class", "edge");
		line.setAttribute("onmouseover", "this.setAttribute('stroke','#00aace')");
		line.setAttribute("onmouseout", "this.setAttribute('stroke','" + color +"')");
		line.setAttribute("id", startNode + "_" + endNode);
		line['fromNode'] = startNode;
		line['toNode'] = endNode;
		
		var start = nodeMap[startNode];
		var end = nodeMap[endNode];
		
		if (!start.outEdges) {
			start['outEdges'] = new Array();
		}
		start.outEdges.push(line);
		
		if (!end.inEdges) {
			end['inEdges'] = new Array();
		}
		end.inEdges.push(line);

		line.setAttributeNS(null, "stroke", color );
		line.setAttributeNS(null, "stroke-width", 3 );
		line.setAttributeNS(null, "marker-end", "url(#triangle)");
		setEdgePoints(line);
		
		svgObj.insertBefore(g, divider);
		edgeList.push(line);
		return line;
	}

	this.createNode = function(id, label, x, y, type) {
		var node = document.createElementNS(svgns, 'g');
		
		nodeMap[id] = node;
		node["enabled"] = true;
		node["type"] = type;
		node["id"] = id;
		node["recurseID"] = -1;
		node.setAttributeNS(null, "id", id);
		node.setAttributeNS(null, "class", "node");
		node.setAttributeNS(null, "font-family", "helvetica");
		node.setAttributeNS(null, "gx", x);
		node.setAttributeNS(null, "gy", y);
		node.setAttributeNS(null, "onmousedown", "this.obj.nodeClicked('"  + id + "')");
		node.setAttributeNS(null, "onmouseover", "this.obj.nodeOver('"  + id + "','over')");
		node.setAttributeNS(null, "onmouseout", "this.obj.nodeOver('"  + id + "','out')");
		node.setAttributeNS(null, "title", label + ": " + type);
		
		node['obj'] = this; 
		node['selected'] = false;
		
		var rect1 = document.createElementNS(svgns, 'rect');
		rect1.setAttributeNS(null, "y", 2);
		rect1.setAttributeNS(null, "x", 2);
		rect1.setAttributeNS(null, "ry", 12);
		rect1.setAttributeNS(null, "width", 20);
		rect1.setAttributeNS(null, "height", 30);
		rect1.setAttributeNS(null, "class", "button " + type);
		rect1.setAttributeNS(null, "style", "width:inherit;fill-opacity:1.0;stroke-opacity:1");

		var text = document.createElementNS(svgns, 'text');
		var textLabel = document.createTextNode(label);
		var guessLength = textLabel.length * 6;
		text.appendChild(textLabel);
		text.setAttributeNS(null, "x", 12);
		text.setAttributeNS(null, "y", 24);
		text.setAttributeNS(null, "height", 10); 
		
		var innerG = document.createElementNS(svgns, 'g');
		innerG.appendChild(rect1);
		innerG.appendChild(text);
		node.appendChild(innerG);
		
		svgObj.appendChild(node);

		var computeText = text.getComputedTextLength();
		rect1.setAttributeNS(null, "width", 20 + computeText);
		
		var halfHeight = 15;
		var halfWidth = computeText/2 + 10;
		innerG.setAttributeNS(null, "transform", "translate(" + (-halfWidth)  + "," + (-halfHeight) + ")");
		node.setAttributeNS(null, "transform", "translate(" + x + "," + y +")")
		node["topRect"] = rect1;
		
		if (x + halfWidth > maxX) {
			maxX = x + halfWidth;
		}
		if(x - halfWidth < minX) {
			minX = x - halfWidth;
		}
		if(y > maxY) {
			maxY = y;
		}
		if(y < minY) {
			minY = y;
		}
		
		// setNodeStyleFromType(node);
		
		return node;
	}
	
	this.panGraph = function(x, y) {
		if (viewer != null) {
			translateX += x;
			translateY += y;
			viewer.setAttributeNS(null, "transform", "translate(" + translateX + "," + translateY + ") scale(" + scale + ")");
		}
	}
	
	this.positionGraph = function(x,y) {
		if (viewer!= null) {
			translateX = x;
			translateY = y;
			viewer.setAttributeNS(null, "transform", "translate(" + translateX + "," + translateY + ") scale(" + scale + ")");
		}
	}
	
	this.getZoomPercent = function() {
		return (scale-minZoom)/(maxZoom - minZoom);
	}

	this.zoomGraphFactor = function(s, x, y) {
		if (viewer != null) {
			
			nx = x;
			ny = y;
			
			scale = scale * s;
			if (scale > maxZoom) {
				scale = maxZoom;
				return;
			}
			else if (minZoom > scale || isNaN(scale) ) {
				scale = minZoom;
				return;
			}
			
			translateX = s*translateX + nx - s*nx;
			translateY = s*translateY + ny - s*ny;

			viewer.setAttributeNS(null, "transform", "translate(" + translateX + "," + translateY + ") scale(" + scale + ") ");
		}
	}
	
	this.zoomGraphPercent = function(percent, x, y) {
		if (viewer != null) {
			var newScale = (maxZoom - minZoom)*percent + minZoom;
			var scaleComponent = newScale/scale;
			
			this.zoomGraphFactor(scaleComponent, x, y);
		}
	}
	
	this.removeClass = function(node, className) {
		var classAttribute = node.getAttribute("class");
		node.setAttribute("class", classAttribute.replace(' ' + className,''));
	}
	
	this.addClass = function(node, className) {
		var classAttribute = node.getAttribute("class");
		if (classAttribute.indexOf(className) == -1) {
			node.setAttribute("class", classAttribute + " " + className);
		}
	}
	
	this.selectNode = function(node) {
		for (var i = 0; i<nodeSelection.length; ++i) {
			var selectedNode = nodeSelection[i];
			if (selectedNode['selected']) {
				selectedNode['selected'] = false;
				//selectedNode.setAttribute("stroke", "DDDDDD");
				var classAttribute = selectedNode.getAttribute("class");
				this.removeClass(selectedNode, 'selected');
				//selectedNode.setAttribute("class", classAttribute.replace(' selected',''));
			}
		}
		nodeSelection= new Array();
		
		node['selected'] = true;
		//node.setAttribute("stroke", "#FFFF00");
		var classAttribute = node.getAttribute("class");
		//node.setAttribute("class", classAttribute + " selected");
		this.addClass(node, 'selected');
		nodeSelection.push(node);
	}
	
	this.nodeClicked = function(id) {
		var node = nodeMap[id];
		if (node) {
			this.selectNode(node);
		}
	};
	
	this.selectAndCenter = function(id) {
		var node = nodeMap[id];
		if (node) {
			this.nodeClicked(id);
			//var x = parseFloat(node.getAttribute("gx"));
			//var y = parseFloat(node.getAttribute("gy"));
			
			//this.positionGraph(-x, -y);
		}
	}
	
	this.nodeOver = function(id, over) {
		var node = nodeMap[id];
		if (over == 'over') {
			nodeEntered = node;
			// debugLabel.textContent = node.getAttribute("title");
			debugLabel.textContent = node.textContent +": "+node.id+": "+node.type;
		}
		else if (over == 'out') {
			nodeEntered = null;
			debugLabel.textContent = "";
		}
	};
	
	this.getSelectedNodes = function() {
		return nodeSelection;
	}
	
	this.getCursorNode = function() {
		return nodeEntered;
	}
	
	this.moveSelectedNodes = function(x, y) {
		// First scale x and y to current x and y
		//node.setAttributeNS(null, "transform", "translate(" + (x - computeText/2 - 10)  + "," + (y - 15) + ")");
		for (var i = 0; i<nodeSelection.length; ++i) {
			var selectedNode = nodeSelection[i];
			var gx = parseFloat(selectedNode.getAttribute("gx"));
			var gy = parseFloat(selectedNode.getAttribute("gy"));
			
			gx += x/scale;
			gy += y/scale;
			
			selectedNode.setAttribute("gx", gx);
			selectedNode.setAttribute("gy", gy);
			selectedNode.setAttributeNS(null, "transform", "translate(" + gx + "," + gy + ")");
			
			if (selectedNode.outEdges) {
				for (var j = 0; j < selectedNode.outEdges.length; ++j) {
					var edge = selectedNode.outEdges[j];
					setEdgePoints(edge);
				}
			}
			if (selectedNode.inEdges) {
				for (var j = 0; j < selectedNode.inEdges.length; ++j) {
					var edge = selectedNode.inEdges[j];
					setEdgePoints(edge);
				}
			}
		}
	}
	
	this.getNodeValues = function() {
		var retValue = "";
		for (var key in nodeMap) {
			var value = nodeMap[key];
			var type = value.type;
			var x = parseFloat(value.getAttribute("gx")).toFixed(1);
			var y = parseFloat(value.getAttribute("gy")).toFixed(1);
			
			retValue += key + ":" + type + ":" + x + "," + y  + ";";
		}
		
		return retValue;
	}
	
	this.getDisabledNodeValues = function() {
		var retValue = "";
		for (var key in nodeMap) {
			var value = nodeMap[key];
			if (!value['enabled']) {
				retValue += key + ",";
			}
		}
		
		return retValue;
	}
}