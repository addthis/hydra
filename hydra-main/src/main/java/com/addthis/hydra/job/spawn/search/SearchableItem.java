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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchableItem {
    private final String[] lines;
    private final String id;

    public SearchableItem(String text, String id) {
        this.lines = text.split("\n");
        this.id = id;
    }

    public Set<TextLocation> search(Pattern pattern) {
        Set<TextLocation> matchSet = new HashSet<>();

        for (int lineNum = 0; lineNum < lines.length; lineNum++) {
            String line = lines[lineNum];
            Matcher m = pattern.matcher(line);
            while (m.find()) {
                matchSet.add(new TextLocation(lineNum, m.start(), m.end()));
            }
        }

        return matchSet;
    }
}
