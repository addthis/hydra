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
package com.addthis.hydra.data.filter.value;

import java.util.HashSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterExclude {

    // TODO: Reduce ridiculous duplication with ValueFilterRequire
    private String excludeFilter(String val, HashSet<String> exactValues, HashSet<String> match, HashSet<String> find, String[] contains) {
        return new ValueFilterExclude().setValue(exactValues).setMatch(match).setContains(contains).setFind(find).filter(val);
    }

    @Test
    public void nullPassThrough() {
        assertEquals(null, excludeFilter(null, null, null, null, null));
        assertEquals("", excludeFilter("", null, null, null, null));
    }

    @Test
    public void exactMatch() {
        HashSet<String> exactValues = new HashSet<String>();
        exactValues.add("bar");
        exactValues.add("bax");
        assertEquals("foo", excludeFilter("foo", exactValues, null, null, null));
        assertEquals("foobarfoo", excludeFilter("foobarfoo", exactValues, null, null, null));
        assertEquals(null, excludeFilter("bar", exactValues, null, null, null));
    }

    @Test
    public void contains() {
        String[] contains = new String[]{"bar", "bax"};
        assertEquals("foo", excludeFilter("foo", null, null, null, contains));
        assertEquals(null, excludeFilter("bar", null, null, null, contains));
    }

    @Test
    public void matches() {
        HashSet<String> matches = new HashSet<String>();
        matches.add("\\d\\d");
        matches.add(".*addthis.com.*");
        assertEquals("foo", excludeFilter("foo", null, matches, null, null));
        assertEquals(null, excludeFilter("www2.addthis.com/live", null, matches, null, null));
        assertEquals(null, excludeFilter("42", null, matches, null, null));
    }

    @Test
    public void find() {
        HashSet<String> find = new HashSet<String>();
        find.add("[^a-z0-9]");
        assertEquals("laputanmachine", excludeFilter("laputanmachine", null, null, find, null));
        assertEquals(null, excludeFilter("-bobhope", null, null, find, null));
    }

    @Test
    public void multi() {
        HashSet<String> exactValues = new HashSet<String>();
        exactValues.add("wam");
        exactValues.add("bam");
        String[] contains = new String[]{"bar", "bax"};
        HashSet<String> matches = new HashSet<String>();
        matches.add("\\d\\d");
        matches.add(".*addthis.com.*");
        assertEquals(null, excludeFilter("wam", exactValues, matches, null, null));
        assertEquals(null, excludeFilter("www2.addthis.com/live", null, matches, null, contains));
        assertEquals(null, excludeFilter("bax", exactValues, null, null, contains));
        assertEquals(null, excludeFilter("bar", exactValues, matches, null, contains));
    }
}
