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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Describes if/where various dependency includes live inside a blob of text
 */
public class IncludeLocations {
    private final Map<String, Set<TextLocation>> locations;

    // Example: %{macro-name}%
    private static final Pattern macroPattern = Pattern.compile("%\\{(.+?)\\}%");
    private static final int macroPatternGroup = 1;

    // Example: %[parameter-name:default-value]%
    private static final Pattern jobParamPattern = Pattern.compile("%\\[(.+?)(:.+?)?\\]%");
    private static final int jobParamPatternGroup = 1;

    public static IncludeLocations forMacros(String text) {
        return new IncludeLocations(text, macroPattern, macroPatternGroup);
    }

    public static IncludeLocations forJobParams(String text) {
        return new IncludeLocations(text, jobParamPattern, jobParamPatternGroup);
    }

    /**
     * @param text
     * @param pattern should match a section of a line for including a dependency
     */
    private IncludeLocations(String text, Pattern pattern) {
        this(text, pattern, 0);
    }

    /**
     * @param text
     * @param pattern should match a section of a line for including a dependency
     * @param patternGroup the group of the matched pattern which contains the name of the dependency being included
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
     * Gives a list of all locations where the dependency by the name of `name` is included in this text
     *
     * @param name the macro which may included
     * @return the set of all locations (possibly empty) where the including happened
     */
    public Set<TextLocation> locationsFor(String name) {
        return locations.getOrDefault(name, ImmutableSet.of());
    }

    /**
     * @return a list of every macro dependency of this text
     */
    public Set<String> dependencies() {
        return locations.keySet();
    }
}
