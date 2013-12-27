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
package com.addthis.hydra.util;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Objects;

public class DirectedGraph<T> {

    public static class Edge<T> {

        @Nonnull
        public final T source;

        @Nonnull
        public final T sink;

        public Edge(@Nonnull T source, @Nonnull T sink) {
            this.source = source;
            this.sink = sink;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Edge)) {
                return false;
            }
            Edge otherEdge = (Edge) other;
            return source.equals(otherEdge.source) && sink.equals(otherEdge.sink);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(source, sink);
        }

    }

    private static <T> Set<T> generateConcurrentSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<T, Boolean>());
    }

    private static class Node<T> {

        @Nonnull
        private final T key;

        @Nonnull
        private final Set<T> sourceEdges;

        @Nonnull
        private final Set<T> sinkEdges;

        Node(@Nonnull T key) {
            this.key = key;
            this.sourceEdges = generateConcurrentSet();
            this.sinkEdges = generateConcurrentSet();
        }
    }

    @Nonnull
    private final ConcurrentHashMap<T, Node<T>> nodes;

    public DirectedGraph() {
        this.nodes = new ConcurrentHashMap<>();
    }

    /**
     * Public API methods.
     */

    /**
     * Retrieve the set of nodes in the graph.
     *
     * @return set of nodes
     */
    public Set<T> getNodes() {
        return nodes.keySet();
    }

    /**
     * Retrieve the source edges for a particular node in the graph.
     *
     * @param key name of a graph node
     * @return either a set of edges or null
     */
    public Set<T> getSourceEdges(T key) {
        Node<T> node = nodes.get(key);
        if (node != null) {
            return node.sourceEdges;
        } else {
            return null;
        }
    }

    /**
     * Retrieve the sink edges for a particular node in the graph.
     *
     * @param key name of a graph node
     * @return either a set of edges or null
     */
    public Set<T> getSinkEdges(T key) {
        Node<T> node = nodes.get(key);
        if (node != null) {
            return node.sinkEdges;
        } else {
            return null;
        }
    }

    public Set<Edge<T>> getAllEdges(Set<T> keys) {
        Set<Edge<T>> retval = new HashSet<>();
        for (T key : keys) {
            Node<T> node = nodes.get(key);
            if (node != null) {
                for (T sink : node.sinkEdges) {
                    Edge<T> edge = new Edge<>(key, sink);
                    retval.add(edge);
                }
                for (T source : node.sourceEdges) {
                    Edge<T> edge = new Edge<>(source, key);
                    retval.add(edge);
                }
            }
        }

        return retval;
    }

    /**
     * Add a new node to the graph.
     *
     * @param key
     * @return true if the graph did not previously contain the node.
     */
    public boolean addNode(T key) {
        return (nodes.putIfAbsent(key, new Node(key)) == null);
    }

    /**
     * Add a directed edge to the graph.
     *
     * @param source edge source
     * @param sink   edge sink
     */
    public void addEdge(T source, T sink) {
        addEdgeHelper(source, sink, true);
        addEdgeHelper(sink, source, false);
    }

    /**
     * Remove a directed edge from the graph.
     *
     * @param source edge source
     * @param sink   edge sink
     */
    public void removeEdge(T source, T sink) {
        Node<T> node = nodes.get(source);

        if (node != null) {
            node.sinkEdges.remove(sink);
        }

        node = nodes.get(sink);

        if (node != null) {
            node.sourceEdges.remove(source);
        }
    }

    /**
     * Remove a node from the graph and its associated edges.
     *
     * @param key
     * @return true if the graph previously contained the node
     */
    public boolean removeNode(T key) {
        Node<T> node = nodes.remove(key);

        if (node == null) {
            return false;
        }

        for (T sink : node.sinkEdges) {
            Node<T> sinkNode = nodes.get(sink);
            if (sinkNode != null) {
                sinkNode.sourceEdges.remove(key);
            }
        }

        for (T source : node.sourceEdges) {
            Node<T> sourceNode = nodes.get(source);
            if (sourceNode != null) {
                sourceNode.sinkEdges.remove(key);
            }
        }

        return true;
    }

    public Set<T> sinksClosure(T key) {
        Set<T> retval = new HashSet<>();
        Set<Node<T>> workSet = new HashSet<>();
        Node<T> root = nodes.get(key);

        if (root != null) {
            workSet.add(root);
        }

        while (!workSet.isEmpty()) {
            Node<T> node = workSet.iterator().next();
            retval.add(node.key);
            workSet.remove(node);
            for (T sink : node.sinkEdges) {
                if (!retval.contains(sink)) {
                    Node<T> sinkNode = nodes.get(sink);
                    if (sinkNode != null) {
                        workSet.add(sinkNode);
                    }
                }
            }
        }

        return retval;
    }

    public Set<T> sourcesClosure(T key) {
        Set<T> retval = new HashSet<>();
        Set<Node<T>> workSet = new HashSet<>();
        Node<T> root = nodes.get(key);

        if (root != null) {
            workSet.add(root);
        }

        while (!workSet.isEmpty()) {
            Node<T> node = workSet.iterator().next();
            retval.add(node.key);
            workSet.remove(node);
            for (T source : node.sourceEdges) {
                if (!retval.contains(source)) {
                    Node<T> sourceNode = nodes.get(source);
                    if (sourceNode != null) {
                        workSet.add(sourceNode);
                    }
                }
            }
        }

        return retval;
    }

    public Set<T> transitiveClosure(T key) {
        Set<T> retval = new HashSet<>();
        Set<Node<T>> workSet = new HashSet<>();
        Node<T> root = nodes.get(key);

        if (root != null) {
            workSet.add(root);
        }

        while (!workSet.isEmpty()) {
            Node<T> node = workSet.iterator().next();
            retval.add(node.key);
            workSet.remove(node);
            for (T source : node.sourceEdges) {
                if (!retval.contains(source)) {
                    Node<T> sourceNode = nodes.get(source);
                    if (sourceNode != null) {
                        workSet.add(sourceNode);
                    }
                }
            }
            for (T sink : node.sinkEdges) {
                if (!retval.contains(sink)) {
                    Node<T> sinkNode = nodes.get(sink);
                    if (sinkNode != null) {
                        workSet.add(sinkNode);
                    }
                }
            }
        }

        return retval;
    }


    /**
     * Tarjan's Algorithm is a graph theory algorithm for finding the strongly
     * connected components of a graph. The algorithm takes a directed graph as input,
     * and produces a partition of the graph's vertices into the graph's strongly
     * connected components.
     * <p/>
     * Any set of two or more strongly connected components is a set of nodes that form
     * a cycle in the graph. A strongly connected set of a single node may or may
     * not be a cycle in a graph.
     */
    class TarjansAlgorithm {

        private int index;
        private final Map<T, Integer> indices;
        private final Map<T, Integer> lowlinks;
        private final Deque<Node<T>> stack;
        private final boolean forward;

        /**
         * Set membership for the stack object.
         * For constant-time stack membership test.
         */
        private final Set<Node<T>> inStack;

        private final Set<T> keys;
        private final Set<Set<T>> components;

        TarjansAlgorithm(boolean forward) {
            this(nodes.keySet(), forward);
        }

        TarjansAlgorithm(Set<T> keys, boolean forward) {
            this.keys = keys;
            this.forward = forward;
            this.indices = new HashMap<>();
            this.lowlinks = new HashMap<>();
            this.stack = new ArrayDeque<>();
            this.inStack = new HashSet<>();
            this.components = new HashSet<>();
        }

        Set<Set<T>> generateComponents() {
            for (T key : keys) {
                Node<T> node = nodes.get(key);
                if (node != null && !indices.containsKey(key)) {
                    strongConnect(node);
                }
            }
            return components;
        }

        private void strongConnect(Node<T> node) {
            T key = node.key;
            indices.put(key, index);
            lowlinks.put(key, index);
            index++;
            stack.push(node);
            inStack.add(node);

            Iterator<T> iterator;

            if (forward) {
                iterator = node.sinkEdges.iterator();
            } else {
                iterator = node.sourceEdges.iterator();
            }

            while (iterator.hasNext()) {
                T next = iterator.next();
                Node<T> nextNode = nodes.get(next);
                if (nextNode != null) {
                    if (!indices.containsKey(next)) {
                        strongConnect(nextNode);
                        lowlinks.put(key, Math.min(lowlinks.get(key),
                                lowlinks.get(next)));
                    } else if (inStack.contains(nextNode)) {
                        lowlinks.put(key, Math.min(lowlinks.get(key),
                                indices.get(next)));
                    }
                }
            }

            int index = indices.get(key);
            int lowlink = lowlinks.get(key);

            if (index == lowlink) {
                Set<T> newComponent = new HashSet<>();
                T next;
                do {
                    Node<T> nextNode = stack.pop();
                    inStack.remove(nextNode);
                    next = nextNode.key;
                    newComponent.add(next);
                }
                while (!next.equals(key));
                components.add(newComponent);
            }
        }
    }

    /**
     * returns all the cycles in the graph that
     * are forward reachable from the key.
     *
     * @param key
     * @return a set of cycles in the graph
     */
    public Set<Set<T>> sinksCycles(T key) {
        Set<Set<T>> strongComponents = cyclesHelper(sinksClosure(key), true);
        return (removeNonCycles(strongComponents));
    }

    /**
     * returns all the cycles in the graph that
     * are backward reachable from the key.
     *
     * @param key
     * @return a set of cycles in the graph
     */
    public Set<Set<T>> sourcesCycles(T key) {
        Set<Set<T>> strongComponents = cyclesHelper(sourcesClosure(key), false);
        return (removeNonCycles(strongComponents));
    }

    /**
     * returns all the cycles in the graph,
     *
     * @return a set of cycles in the graph
     */
    public Set<Set<T>> allCycles() {
        Set<Set<T>> strongComponents = cyclesHelper(nodes.keySet(), true);
        return (removeNonCycles(strongComponents));
    }

    public Set<Set<T>> stronglyConnectedComponents() {
        Set<Set<T>> strongComponents = cyclesHelper(nodes.keySet(), true);
        return strongComponents;
    }

    /**
     * Private helper methods.
     */

    private Set<Set<T>> cyclesHelper(Set<T> domain, boolean forward) {
        TarjansAlgorithm tarjan = new TarjansAlgorithm(domain, forward);
        Set<Set<T>> strongComponents = tarjan.generateComponents();
        return strongComponents;
    }

    private Set<Set<T>> removeNonCycles(Set<Set<T>> strongComponents) {
        Iterator<Set<T>> iterator = strongComponents.iterator();
        while (iterator.hasNext()) {
            Set<T> component = iterator.next();
            if (component.size() == 1) {
                T single = component.iterator().next();
                Node<T> node = nodes.get(single);
                if (node == null || !node.sinkEdges.contains(single)) {
                    iterator.remove();
                }
            }
        }
        return strongComponents;
    }

    private void addEdgeHelper(T nodeGet, T nodePut, boolean sinkEdges) {
        Node<T> node = nodes.get(nodeGet);
        if (node == null) {
            Node<T> newNode = new Node(nodeGet);
            Node<T> prevNode = nodes.putIfAbsent(nodeGet, newNode);
            node = (prevNode == null) ? newNode : prevNode;
        }
        if (sinkEdges) {
            node.sinkEdges.add(nodePut);
        } else {
            node.sourceEdges.add(nodePut);
        }
    }

    /**
     * Methods with package level visibility. Intended for unit testing.
     */

    boolean testEdge(T source, T sink) {
        Node<T> node = nodes.get(source);
        if (node == null) {
            return false;
        }
        return node.sinkEdges.contains(sink);
    }

    boolean testEdgeReverse(T source, T sink) {
        Node<T> node = nodes.get(sink);
        if (node == null) {
            return false;
        }
        return node.sourceEdges.contains(source);
    }


}
