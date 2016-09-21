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
package com.addthis.hydra.job.spawn.search;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.addthis.hydra.job.entity.JobMacro;
import com.addthis.hydra.util.DirectedGraph;

/**
 * Represents the dependencies between a set of {@link JobMacro}s
 */
public class JobMacroGraph {

    /**
     * Directed graph from a macro to other macros that it includes
     */
    private final DirectedGraph<String> graph;

    /**
     * Key is macro name; value is the locations of all other macros included in the named macro
     */
    private final Map<String, IncludeLocations> includeLocations;


    public JobMacroGraph(Map<String, JobMacro> macros) {
        graph = new DirectedGraph<>();
        includeLocations = new HashMap<>();

        for (String macroName : macros.keySet()) {
            JobMacro jobMacro = macros.get(macroName);

            graph.addNode(macroName);
            IncludeLocations locations = IncludeLocations.forMacros(jobMacro.getMacro());
            includeLocations.put(macroName, locations);

            for (String depName : locations.dependencies()) {
                graph.addEdge(macroName, depName);
            }
        }
    }

    /**
     * Returns the given macro and all macros that it includes (recursively).
     * <p/>
     * For example, if A includes B, and B includes C, calling this method on A will return A, B and C.
     *
     * @param macroName     the name of the marco for which to get the dependencies
     */
    public Set<String> getDependencies(String macroName) {
        return graph.sinksClosure(macroName);
    }

    /**
     * Returns the locations of all macros included in the given macro.
     *
     * @param macroName     the name of a macro which possibly includes other macros
     * @return              all locations where other macros are included, or an empty set if nothing is included
     */
    public IncludeLocations getIncludeLocations(String macroName) {
        return includeLocations.get(macroName);
    }
}
