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
package com.addthis.hydra.job;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.awt.geom.Point2D;

import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;


public class FlowGraph {

    private Map<String, Flow> flowMap;
    private Set<String> flowsLaidOut;

    public FlowGraph() {
        this.flowMap = new HashMap<String, Flow>();
        this.flowsLaidOut = new HashSet<String>();
    }

    public DisplayFlowNode build(String rootId) {
        DisplayFlowNode displayFlow = new DisplayFlowNode(rootId); //create display root based on root id
        fillFlow(displayFlow, rootId);
        displayFlow.validateFlow();

        //Layout DisplayNode tree
        SugiyamaLayout layout = new SugiyamaLayout(displayFlow);
        layout.setLayout();

        return displayFlow;
    }

    public void addFlow(String rootId, String... depIds) {
        if (!this.flowMap.containsKey(rootId)) {
            this.flowMap.put(rootId, new IndividualFlow(rootId));
        }

        if (depIds.length > 0) {
            Flow root = this.flowMap.get(rootId);
            List<Flow> deps = new ArrayList<>(root.getChildren());
            for (String depId : depIds) {
                addFlow(depId);
                Flow dep = this.flowMap.get(depId);
                deps.add(dep);
            }

            Flow[] flowDeps = new Flow[deps.size()];
            deps.toArray(flowDeps);

            root = new MultipleDependencyFlow(root.getDepender(), flowDeps);

            this.flowMap.put(rootId, root);
        }
    }

    private void fillFlow(DisplayFlowNode displayFlow, String flowName) {
        //This is to not get stuck in a cycle
        if (this.flowsLaidOut.contains(flowName)) {
            return;
        } else {
            this.flowsLaidOut.add(flowName);
        }

        Flow flow = this.flowMap.get(flowName);
        List<String> dependencies = new ArrayList<>();

        for (Flow depFlow : flow.getChildren()) {
            dependencies.add(depFlow.getName());
            fillFlow(displayFlow, depFlow.getName());
        }

        displayFlow.addDependencies(flow.getName(), dependencies);
        displayFlow.setStatus(flow.getName(), "IDLE");
    }

    public static class FlowNode {

        public static final String NORMAL = "normal";
        public static final String DISABLED = "disabled";
        public static final String RUNNING = "running";
        public static final String FAILED = "failed";
        public static final String SUCCEEDED = "succeeded";

        private String alias;
        private Set<String> dependencies = new HashSet<String>();
        private Set<String> dependents = new HashSet<String>();

        private int level = 0;
        private String status = NORMAL;
        private Point2D position = new Point2D.Double();

        public FlowNode(String alias) {
            this.alias = alias;
        }

        public FlowNode(FlowNode other) {
            this.alias = other.alias;
            this.dependencies.addAll(other.dependencies);
            this.dependents.addAll(other.dependents);
            this.level = other.level;
            this.status = other.status;
            this.position = other.position;
        }

        public void setDependencies(Set<String> dependencies) {
            this.dependencies = dependencies;
        }

        public Set<String> getDependencies() {
            return dependencies;
        }

        public Set<String> getDependents() {
            return dependents;
        }

        public void addDependent(String dependent) {
            dependents.add(dependent);
        }

        public String getAlias() {
            return alias;
        }

        public int getLevel() {
            return level;
        }

        public void setLevel(int level) {
            this.level = level;
        }

        public void setPosition(double x, double y) {
            position = new Point2D.Double(x, y);
        }

        public double getX() {
            return position.getX();
        }

        public double getY() {
            return position.getY();
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

    /**
     * A grouping of flows.
     * <p/>
     * For example, if you had two functions f(x) and g(x) that you wanted to execute together, you
     * could conceivably create a new function h(x) = { f(x); g(x); }.  That is essentially what
     * this class does with ExecutableFlow objects.
     * <p/>
     * You should never really have to create one of these directly.  Try to use MultipleDependencyFlow
     * instead.
     */
    public static class GroupedFlow implements Flow {

        private Flow[] flows;
        private Flow[] sortedFlows;

        public GroupedFlow(Flow... flows) {
            this.flows = flows;
            this.sortedFlows = Arrays.copyOf(this.flows, this.flows.length);
            Arrays.sort(this.sortedFlows, new Comparator<Flow>() {
                @Override
                public int compare(Flow o1, Flow o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });
        }

        @Override
        public String getName() {
            Joiner joiner = Joiner.on(",");
            return joiner.join(
                    Iterables.transform(Arrays.asList(flows), new Function<Flow, String>() {
                        @Override
                        public String apply(Flow flow) {
                            return flow.getName();
                        }
                    }).iterator(),
                    " + "
            );
        }

        @Override
        public boolean hasChildren() {
            return true;
        }

        @Override
        public List<Flow> getChildren() {
            return Arrays.asList(sortedFlows);
        }


        @Override
        public String toString() {
            return "GroupedExecutableFlow{" +
                   "flows=" + (flows == null ? null : Arrays.asList(flows)) +
                   '}';
        }

        @Override
        public Flow getDepender() {
            return null;
        }
    }

    public static class IndividualFlow implements Flow {

        private String name;

        public IndividualFlow(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public boolean hasChildren() {
            return false;
        }

        @Override
        public List<Flow> getChildren() {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "IndividualJobExecutableFlow{" +
                   "job=" + name +
                   '}';
        }

        @Override
        public Flow getDepender() {
            return this;
        }
    }

    /**
     * A flow that provides an API for creating a dependency graph.
     * <p/>
     * The class takes a minimum of two Flow objects on the constructor and builds
     * a dependency relationship between them.  The first Flow object dependant on the
     * others (second, third, ...).
     * <p/>
     * That is, if you have flows A, B and C, and you want A to depend on the execution of
     * B and C, simply constructor a
     * <p/>
     * new MultipleDependencyFlow(A, B, C);
     * <p/>
     * This class makes use of ComposedFlow and GroupedFlow under the covers
     * to ensure this behavior, but it exposes a more stream-lined "view" of the dependency
     * graph that makes it easier to reason about traversals of the resultant DAG.
     */
    public static class MultipleDependencyFlow implements Flow {

        private GroupedFlow dependeesGrouping;
        private ComposedFlow actualFlow;
        private Flow depender;

        public MultipleDependencyFlow(Flow depender, Flow... dependees) {
            this.depender = depender;
            dependeesGrouping = new GroupedFlow(dependees);
            actualFlow = new ComposedFlow(this.depender, dependeesGrouping);
        }

        @Override
        public String getName() {
            return actualFlow.getName();
        }

        @Override
        public boolean hasChildren() {
            return true;
        }

        @Override
        public List<Flow> getChildren() {
            return dependeesGrouping.getChildren();
        }

        public Flow getDepender() {
            return this.depender;
        }

        @Override
        public String toString() {
            return "MultipleDependencyExecutableFlow{" +
                   "dependeesGrouping=" + dependeesGrouping +
                   ", actualFlow=" + actualFlow +
                   '}';
        }
    }

    public static class SugiyamaLayout {

        HashMap<Integer, ArrayList<Node>> levelMap = new HashMap<>();
        LinkedHashMap<String, JobNode> nodesMap = new LinkedHashMap<>();

        public static final float LEVEL_HEIGHT = 120;
        public static final float LEVEL_WIDTH = 80;
        public static final float LEVEL_WIDTH_ADJUSTMENT = 5;

        public final DisplayFlowNode flow;

        public SugiyamaLayout(DisplayFlowNode flow) {
            this.flow = flow;
        }

        public void setLayout() {
            nodesMap.clear();
            int longestWord = 0;
            for (FlowNode flowNode : flow.getFlowNodes()) {
                longestWord = Math.max(flowNode.getAlias().length(), longestWord);
                nodesMap.put(flowNode.getAlias(), new JobNode(flowNode));
            }

            for (JobNode jobNode : nodesMap.values()) {
                layerNodes(jobNode);
            }

            // Re-arrange top down!
            int size = levelMap.size();
            ArrayList<Node> list = levelMap.get(0);
            float count = 0;
            for (Node node : list) {
                node.setPosition(count);
                count += 1;
            }
            for (int i = 1; i < size; ++i) {
                //barycenterMethodUncross(levelMap.get(i), true);
                barycenterMethodUncross(levelMap.get(i));
            }

            float widthAdjustment = LEVEL_WIDTH + longestWord * LEVEL_WIDTH_ADJUSTMENT;
            // Adjust level
            for (Map.Entry<Integer, ArrayList<Node>> entry : levelMap.entrySet()) {
                ArrayList<Node> nodes = entry.getValue();
                Integer level = entry.getKey();

                float offset = -((float) nodes.size() / 2);
                for (Node node : nodes) {
                    if (node instanceof JobNode) {
                        FlowNode flowNode = ((JobNode) node).getFlowNode();
                        flowNode.setPosition(offset * widthAdjustment, level * LEVEL_HEIGHT);
                    }

                    offset += 1;
                }

            }

            flow.setLayedOut(true);
        }

        private void uncrossLayer(ArrayList<Node> free, boolean topDown) {
            // Using median method
            if (topDown) {
                for (Node node : free) {
                    float median = getMedian(node.getDependencies());
                    System.out.println("getMedian " + median);
                    node.setPosition(median);
                }
            } else {
                for (Node node : free) {
                    float median = getMedian(node.getDependents());
                    //System.out.println("getMedian " + median);
                    node.setPosition(median);
                }
            }

            Collections.sort(free);
        }

        private void barycenterMethodUncross(ArrayList<Node> free) {
            int numOnLevel = free.size();
            for (Node node : free) {
                float average = findAverage(node.getDependencies());
                node.setPosition(average);
                node.setNumOnLevel(numOnLevel);
            }

            Collections.sort(free);
            reorder(free);
        }

        private void reorder(ArrayList<Node> nodes) {
            int count = 1;
            for (int i = 0; i < nodes.size(); ++i) {
                Node node = nodes.get(i);
                node.setPosition(i);
            }
        }

        private float findAverage(List<Node> nodes) {
            float sum = 0;
            for (Node node : nodes) {
                sum += node.getPosition();
            }

            return sum / (nodes.size());
        }

        private float getMedian(List<Node> dependents) {
            int length = dependents.size();
            if (length == 0) {
                return 0;
            }

            float[] position = new float[length];
            int i = 0;
            for (Node dep : dependents) {
                position[i] = dep.getPosition();
                ++i;
            }

            Arrays.sort(position);
            int index = position.length / 2;
            if ((length % 2) == 1) {
                return position[index];
            } else {
                return (position[index - 1] + position[index]) / 2;
            }
        }

        private void layerNodes(JobNode node) {
            FlowNode flowNode = node.getFlowNode();
            int level = node.getLevel();

            ArrayList<Node> levelList = levelMap.get(level);
            if (levelList == null) {
                levelList = new ArrayList<Node>();
                levelMap.put(level, levelList);
            }

            levelList.add(node);

            for (String dep : flowNode.getDependents()) {
                JobNode depNode = nodesMap.get(dep);
                if (depNode.getLevel() - node.getLevel() > 0) {
//                  addDummyNodes(node, depNode);
                    addDummyNodes(depNode, node);
                } else {
//                  depNode.addDependecy(node);
//                  node.addDependent(depNode);
                    node.addDependecy(depNode);
                    depNode.addDependent(node);
                }
            }
        }

        private void addDummyNodes(JobNode from, JobNode to) {
            int fromLevel = from.getLevel();
            int toLevel = to.getLevel();

            Node fromNode = from;
            for (int i = fromLevel; i < toLevel; ++i) {
                DummyNode dummyNode = new DummyNode(i);

                ArrayList<Node> levelList = levelMap.get(i);
                if (levelList == null) {
                    levelList = new ArrayList<Node>();
                    levelMap.put(i, levelList);
                }
                levelList.add(dummyNode);

                dummyNode.addDependecy(fromNode);
                fromNode.addDependent(dummyNode);
                fromNode = dummyNode;
            }

            to.addDependecy(from);
            fromNode.addDependent(to);
        }

        private class Node implements Comparable {

            private List<Node> dependents = new ArrayList<Node>();
            private List<Node> dependencies = new ArrayList<Node>();
            private float position = 0;
            private int numOnLevel = 0;

            public void addDependent(Node dependent) {
                dependents.add(dependent);
            }

            public void addDependecy(Node dependency) {
                dependencies.add(dependency);
            }

            public List<Node> getDependencies() {
                return dependencies;
            }

            public List<Node> getDependents() {
                return dependents;
            }

            public float getPosition() {
                return position;
            }

            private void setPosition(float pos) {
                position = pos;
            }

            @Override
            public int compareTo(Object arg0) {
                // TODO Auto-generated method stub
                Node other = (Node) arg0;
                Float pos = position;

                int comp = pos.compareTo(other.position);
                if (comp == 0) {
                    // Move larger # one to center.
                    int midpos = numOnLevel / 2;


                    //              // First priority... # of out nodes.
                    if (this.dependents.size() > other.dependents.size()) {
                        return 1;
                    } else if (this.dependents.size() < other.dependents.size()) {
                        return -1;
                    }

                    // Second priority... # of out nodes
                    if (this.dependencies.size() > other.dependencies.size()) {
                        return 1;
                    } else if (this.dependencies.size() < other.dependencies.size()) {
                        return -1;
                    }

                    if (this instanceof DummyNode) {
                        if (arg0 instanceof DummyNode) {
                            return 0;
                        }
                        return -1;
                    } else if (arg0 instanceof DummyNode) {
                        return 1;
                    }
                }

                return comp;
            }

            public void setNumOnLevel(int numOnLevel) {
                this.numOnLevel = numOnLevel;
            }

            public int getNumOnLevel() {
                return numOnLevel;
            }

        }

        private class JobNode extends Node {

            private FlowNode flowNode;

            public JobNode(FlowNode flowNode) {
                this.flowNode = flowNode;
            }

            public FlowNode getFlowNode() {
                return flowNode;
            }

            public int getLevel() {
                return flowNode.getLevel();
            }
        }

        private class DummyNode extends Node {

            private int level = 0;

            public DummyNode(int level) {
                this.level = level;
            }
        }

        public class LayoutData {

            final FlowNode flowNode;

            public LayoutData(FlowNode flow) {
                flowNode = flow;
            }
        }

    }

    /**
     * A "composition" of flows.  This is composition in the functional sense.
     * <p/>
     * That is, the composition of two functions f(x) and g(x) is equivalent to f(g(x)).
     * Similarly, the composition of two ExecutableFlows A and B will result in a dependency
     * graph A -> B, meaning that B will be executed and complete before A.
     * <p/>
     * If B fails, A never runs.
     * <p/>
     * You should never really have to create one of these directly.  Try to use MultipleDependencyFlow
     * instead.
     */
    public static class ComposedFlow implements Flow {

        private Flow depender;
        private Flow dependee;

        public ComposedFlow(Flow depender, Flow dependee) {
            this.depender = depender;
            this.dependee = dependee;
        }

        @Override
        public String getName() {
            return depender.getName();
        }

        @Override
        public boolean hasChildren() {
            return true;
        }

        @Override
        public List<Flow> getChildren() {
            return Arrays.asList(dependee);
        }

        @Override
        public String toString() {
            return "ComposedExecutableFlow{" +
                   "depender=" + depender +
                   ", dependee=" + dependee +
                   '}';
        }

        @Override
        public Flow getDepender() {
            return null;
        }
    }

    public static class Dependency {

        private final FlowNode dependent;
        private final FlowNode dependency;

        public Dependency(FlowNode dependency, FlowNode dependent) {
            this.dependent = dependent;
            this.dependency = dependency;
        }

        public FlowNode getDependent() {
            return dependent;
        }

        public FlowNode getDependency() {
            return dependency;
        }
    }

    public static class DisplayFlowNode {

        private final String id;
        private boolean isLayedOut = false;
        private LinkedHashMap<String, FlowNode> flowItems = new LinkedHashMap<String, FlowNode>();

        private ArrayList<String> errorMessages = new ArrayList<String>();
        private boolean hasValidated = false;

        private long timestamp;
        private long layoutTimestamp;

        public DisplayFlowNode(String id, DisplayFlowNode toClone) {
            this.id = id;

            isLayedOut = toClone.isLayedOut;
            for (Map.Entry<String, FlowNode> node : toClone.flowItems.entrySet()) {
                FlowNode cloned = new FlowNode(node.getValue());
                flowItems.put(node.getKey(), cloned);
            }

            this.timestamp = toClone.timestamp;
            this.layoutTimestamp = toClone.layoutTimestamp;
            this.hasValidated = toClone.hasValidated;
        }

        public DisplayFlowNode(String id) {
            this.id = id;
        }

        public void setLastModifiedTime(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getLastModifiedTime() {
            return timestamp;
        }

        public void setLastLayoutModifiedTime(long timestamp) {
            this.layoutTimestamp = timestamp;
        }

        public long getLastLayoutModifiedTime() {
            return layoutTimestamp;
        }

        public List<Dependency> getDependencies() {
            ArrayList<Dependency> dependencies = new ArrayList<Dependency>();
            for (FlowNode node : flowItems.values()) {
//              for (String dependents : node.getDependents() ) {
                for (String dependency : node.getDependencies()) {
                    dependencies.add(new Dependency(node, flowItems.get(dependency)));
                }
            }
            return dependencies;
        }

        public void setPosition(String id, float x, float y) {
            FlowNode node = flowItems.get(id);
            node.setPosition(x, y);
        }

        public void setStatus(String id, String status) {
            FlowNode node = flowItems.get(id);
            node.setStatus(status);
        }

        /**
         * Add job and its dependencies to the flow graph.
         *
         * @param id
         * @param dependency
         */
        public void addDependencies(String id, List<String> dependency) {
            FlowNode node = flowItems.get(id);
            if (node == null) {
                node = new FlowNode(id);
                flowItems.put(id, node);
            }

            if (node.getDependencies() != null) {
                errorMessages.add("Job " + id + " has multiple dependency entries in this flow.");
            }
            HashSet<String> set = new HashSet<String>();
            set.addAll(dependency);
            node.setDependencies(set);

            // Go through the node's dependencies and add the node as a dependent.
            for (String dep : dependency) {
                if (dep.equals(id)) {
                    errorMessages.add("Job " + id + " has defined itself as a dependency.");
                    continue;
                }

                FlowNode parentNode = flowItems.get(dep);
                if (parentNode == null) {
                    parentNode = new FlowNode(dep);
                    flowItems.put(dep, parentNode);
                }
                parentNode.addDependent(id);
            }
        }

        /**
         * Checks flow for cyclical errors and validates the jobs within this flow.
         *
         * @return
         */
        public boolean validateFlow() {
            if (hasValidated) {
                return errorMessages.isEmpty();
            }
            hasValidated = true;
            // Find root nodes without dependencies and then employ breath first search to find cycles in the DAG
            ArrayList<FlowNode> topNodes = new ArrayList<FlowNode>();
            for (FlowNode node : flowItems.values()) {
                if (node.getDependencies() == null || node.getDependencies().isEmpty()) {
                    topNodes.add(node);
                }
            }

            HashSet<String> visited = new HashSet<String>();

            for (FlowNode node : topNodes) {
                visited.add(node.getAlias());
                if (!graphLayer(visited, node, 0)) {
                    break;
                }
                visited.remove(node.getAlias());
            }

            return errorMessages.isEmpty();
        }

        // Find cycles and mark the node level.
        private boolean graphLayer(HashSet<String> visited, FlowNode node, int level) {
            int currentLevel = Math.max(level, node.getLevel());
            node.setLevel(currentLevel);

            for (String dep : node.getDependents()) {
                if (visited.contains(dep)) {
                    errorMessages.add("Found cycle at " + node.getAlias());
                    return false;
                }

                visited.add(dep);
                if (!graphLayer(visited, flowItems.get(dep), currentLevel + 1)) {
                    return false;
                }

                visited.remove(dep);
            }

            return true;
        }

        private boolean graphLayer2(HashSet<String> visited, FlowNode node, int level) {
            int currentLevel = Math.max(level, node.getLevel());
            //      int currentLevel = Math.min(level,node.getLevel());
            node.setLevel(currentLevel);

            for (String dep : node.getDependents()) {
                if (visited.contains(dep)) {
                    errorMessages.add("Found cycle at " + node.getAlias());
                    return false;
                }

                visited.add(dep);
                if (!graphLayer(visited, flowItems.get(dep), currentLevel + 1)) {
                    return false;
                }

                visited.remove(dep);
            }

            return true;
        }

        public List<String> errorMessages() {
            return errorMessages;
        }

        public List<FlowNode> getFlowNodes() {
            return new ArrayList<FlowNode>(flowItems.values());
        }

        public FlowNode getFlowNode(String alias) {
            return flowItems.get(alias);
        }

        public void printFlow() {
            System.out.println("Flow " + this.getId());
            for (FlowNode flow : flowItems.values()) {
                System.out.print(" " + flow.getLevel() + " Job " + flow.getAlias() + " ->[");
                for (String dependents : flow.getDependents()) {
                    System.out.print(dependents + ", ");
                }
                System.out.print("]\n");
            }
        }

        public void setLayedOut(boolean isLayedOut) {
            this.isLayedOut = isLayedOut;
        }

        public boolean isLayedOut() {
            return isLayedOut;
        }

        public String getId() {
            return this.id;
        }


        public JSONObject toJSON() throws Exception {
            JSONObject jsonFlow = new JSONObject();
            jsonFlow.put("flow_id", this.getId());

            JSONArray jsonNodes = new JSONArray();

            for (FlowNode node : this.getFlowNodes()) {
                JSONObject jsonNode = new JSONObject();
                jsonNode.put("name", node.getAlias());
                jsonNode.put("x", node.getX());
                jsonNode.put("y", node.getY());
                jsonNode.put("status", node.getStatus());
                jsonNodes.put(jsonNode);
            }

            JSONArray jsonDependency = new JSONArray();

            for (Dependency dep : this.getDependencies()) {
                JSONObject jsonDep = new JSONObject();
                jsonDep.put("dependency", dep.getDependency().getAlias());
                jsonDep.put("dependent", dep.getDependent().getAlias());
                jsonDependency.put(jsonDep);
            }

            jsonFlow.put("nodes", jsonNodes);
            jsonFlow.put("dependencies", jsonDependency);

            return jsonFlow;
        }
    }

    public static interface Flow {

        public String getName();

        public boolean hasChildren();

        public List<Flow> getChildren();

        public Flow getDepender();
    }
}
