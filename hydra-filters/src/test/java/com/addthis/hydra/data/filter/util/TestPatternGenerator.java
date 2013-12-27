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
package com.addthis.hydra.data.filter.util;

import java.util.regex.Pattern;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPatternGenerator {

    @Test
    public void testAcceptStrings() {
        String[] input = new String[3];
        input[0] = "foo";
        input[1] = "bar";
        input[2] = "baz";
        String regex = PatternGenerator.acceptStrings(input);

        Pattern pattern = Pattern.compile(regex);

        assertTrue(pattern.matcher("foo").matches());
        assertTrue(pattern.matcher("bar").matches());
        assertTrue(pattern.matcher("baz").matches());
        assertFalse(pattern.matcher("foobaz").matches());
        assertFalse(pattern.matcher("").matches());
    }

    @Test
    public void testSubsequence() {
        String[] input = new String[3];
        input[0] = "foo";
        input[1] = "bar";
        input[2] = "baz";
        String regex = PatternGenerator.acceptStrings(input);

        Pattern pattern = Pattern.compile(regex);

        assertTrue(pattern.matcher("foo").find());
        assertTrue(pattern.matcher("bar").find());
        assertTrue(pattern.matcher("baz").find());
        assertTrue(pattern.matcher("hellofoo").find());
        assertTrue(pattern.matcher("hellofooworld").find());
        assertFalse(pattern.matcher("").find());
    }

}
