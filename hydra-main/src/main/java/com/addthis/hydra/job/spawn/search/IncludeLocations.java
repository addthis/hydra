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

import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Describes if/where various dependency includes live (e.g. marco/parameters inside a job config or macro).
 * <p/>
 * This calss is a wrapper of an internal map of marco/param names to their locations (in a body of text). Note that
 * the location represents that of the full pattern, which includes the <code>%{...}%/%[...]%</code> enclosure.
 * <p/>
 * Static methods are provided for processing given text, such as job config or macro, to find included marcos or job
 * parameters.
 */
public class IncludeLocations {

    private final Map<String, Set<TextLocation>> locations;

    // Example: %{macro-name}%
    private static final Pattern MACRO_PATTERN = Pattern.compile("%\\{(.+?)\\}%");
    private static final int MACRO_PATTERN_GROUP = 1;

    // Example: %[parameter-name:default-value]%
    private static final Pattern JOB_PARAM_PATTERN = Pattern.compile("%\\[(.+?)(:.+?)?\\]%");
    private static final int JOB_PARAM_PATTERN_GROUP = 1;

    /** Finds all macro locations in the given text. */
    public static IncludeLocations forMacros(String text) {
        return new IncludeLocations(text, MACRO_PATTERN, MACRO_PATTERN_GROUP);
    }

    /** Finds all job parameter locations in the given text. */
    public static IncludeLocations forJobParams(String text) {
        return new IncludeLocations(text, JOB_PARAM_PATTERN, JOB_PARAM_PATTERN_GROUP);
    }

    /**
     * @param text          the text to search
     * @param pattern       should match a section of a line for including a dependency
     * @param patternGroup  the group of the matched pattern which contains the name of the dependency being included
     */
    private IncludeLocations(String text, Pattern pattern, int patternGroup) {
        if (patternGroup < 0) {
            throw new IllegalArgumentException("The patternGroup cannot be negative");
        }

        Map<String, Set<TextLocation>> locs = new TreeMap<>();
        Scanner s = new Scanner(text);

        int lineNum = 0;
        while (s.hasNextLine()) {
            Matcher m = pattern.matcher(s.nextLine());
            while (m.find()) {
                String depName = m.group(patternGroup);
                TextLocation match = new TextLocation(lineNum, m.start(), m.end());

                Set<TextLocation> matches = locs.get(depName);
                if (matches == null) {
                    matches = new HashSet<>();
                    locs.put(depName, matches);
                }

                matches.add(match);
            }

            lineNum++;
        }

        locations = ImmutableMap.copyOf(locs);
    }

    /**
     * Returns the locations of the included <code>name</code>
     *
     * @param name  the macro/param name which may included
     * @return empty set if "name" is not included
     */
    public Set<TextLocation> locationsFor(String name) {
        return locations.getOrDefault(name, ImmutableSet.of());
    }

    /**
     * Returns all the included macro/parameter names.
     */
    public Set<String> dependencies() {
        return locations.keySet();
    }
}
