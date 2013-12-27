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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestDirectedGraph {

    @Test
    public void testAddEdge() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        assertFalse(graph.testEdge("foo", "bar"));
        assertFalse(graph.testEdgeReverse("foo", "bar"));
        graph.addEdge("foo", "bar");
        assertTrue(graph.testEdge("foo", "bar"));
        assertTrue(graph.testEdgeReverse("foo", "bar"));

        graph = new DirectedGraph<>();
        graph.addEdge("foo", "foo");
        assertTrue(graph.testEdge("foo", "foo"));
        assertTrue(graph.testEdgeReverse("foo", "foo"));

    }

    @Test
    public void testRemoveEdge() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        graph.addEdge("foo", "bar");
        assertTrue(graph.testEdge("foo", "bar"));
        assertTrue(graph.testEdgeReverse("foo", "bar"));
        graph.removeEdge("foo", "bar");
        assertFalse(graph.testEdge("foo", "bar"));
        assertFalse(graph.testEdgeReverse("foo", "bar"));

        graph = new DirectedGraph<>();
        graph.addEdge("foo", "foo");
        assertTrue(graph.testEdge("foo", "foo"));
        assertTrue(graph.testEdgeReverse("foo", "foo"));
        graph.removeEdge("foo", "foo");
        assertFalse(graph.testEdge("foo", "foo"));
        assertFalse(graph.testEdgeReverse("foo", "foo"));
    }

    @Test
    public void testRemoveNode() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        graph.addEdge("foo", "bar");
        graph.addEdge("foo", "baz");
        graph.addEdge("quux", "foo");
        graph.removeNode("foo");
        assertFalse(graph.testEdge("foo", "bar"));
        assertFalse(graph.testEdgeReverse("foo", "bar"));
        assertFalse(graph.testEdge("foo", "baz"));
        assertFalse(graph.testEdgeReverse("foo", "baz"));
        assertFalse(graph.testEdge("quux", "foo"));
        assertFalse(graph.testEdgeReverse("quux", "foo"));

        graph = new DirectedGraph<>();
        graph.addEdge("foo", "foo");
        graph.removeNode("foo");
        assertFalse(graph.testEdge("foo", "foo"));
        assertFalse(graph.testEdgeReverse("foo", "foo"));
    }

    @Test
    public void testForwardClosure() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        graph.addEdge("D", "D");
        graph.addEdge("D", "F");
        graph.addEdge("D", "G");
        graph.addEdge("G", "H");
        graph.addEdge("H", "F");
        graph.addEdge("F", "D");
        graph.addEdge("A", "B");
        graph.addEdge("B", "G");

        String[] resultArray = {"D", "F", "G", "H"};
        Set<String> results = new HashSet<String>(Arrays.asList(resultArray));
        assertEquals(results, graph.sinksClosure("D"));
    }

    @Test
    public void testBackwardClosure() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        graph.addEdge("D", "D");
        graph.addEdge("D", "F");
        graph.addEdge("D", "G");
        graph.addEdge("G", "H");
        graph.addEdge("H", "F");
        graph.addEdge("A", "B");
        graph.addEdge("B", "G");

        String[] resultArray = {"A", "B", "G", "D"};
        Set<String> results = new HashSet<String>(Arrays.asList(resultArray));
        assertEquals(results, graph.sourcesClosure("G"));
    }

    @Test
    public void testTarjansAlgorithm() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        graph.addEdge("B", "A");
        graph.addEdge("A", "C");
        graph.addEdge("C", "B");
        graph.addEdge("D", "C");
        graph.addEdge("D", "B");
        graph.addEdge("E", "B");
        graph.addEdge("E", "G");
        graph.addEdge("G", "E");
        graph.addEdge("F", "E");
        graph.addEdge("F", "D");
        graph.addEdge("D", "F");
        graph.addEdge("H", "G");
        graph.addEdge("H", "F");
        Set<Set<String>> components = graph.stronglyConnectedComponents();
        assertEquals(4, components.size());
        String[] component1 = {"C", "B", "A"};
        String[] component2 = {"E", "G"};
        String[] component3 = {"H"};
        String[] component4 = {"D", "F"};
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component1))));
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component2))));
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component3))));
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component4))));
    }

    @Test
    public void testFindAllCycles() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        graph.addEdge("B", "A");
        graph.addEdge("A", "C");
        graph.addEdge("C", "B");
        graph.addEdge("D", "C");
        graph.addEdge("D", "B");
        graph.addEdge("E", "B");
        graph.addEdge("E", "G");
        graph.addEdge("G", "E");
        graph.addEdge("F", "E");
        graph.addEdge("F", "D");
        graph.addEdge("D", "F");
        graph.addEdge("H", "G");
        graph.addEdge("H", "F");
        Set<Set<String>> components = graph.allCycles();
        assertEquals(3, components.size());
        String[] component1 = {"C", "B", "A"};
        String[] component2 = {"E", "G"};
        String[] component3 = {"D", "F"};
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component1))));
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component2))));
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component3))));
    }

    @Test
    public void testFindBackwardCycles() {
        DirectedGraph<String> graph = new DirectedGraph<>();
        graph.addEdge("A", "B");
        graph.addEdge("B", "C");
        graph.addEdge("C", "B");
        graph.addEdge("D", "A");
        graph.addEdge("D", "E");
        graph.addEdge("E", "D");
        Set<Set<String>> components = graph.sourcesCycles("A");
        assertEquals(1, components.size());
        String[] component1 = {"D", "E"};
        assertTrue(components.contains(new HashSet<String>(Arrays.asList(component1))));
    }


}
