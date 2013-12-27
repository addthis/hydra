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
package com.addthis.hydra.data.util;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class TestTokenizer {


    private class Tester {

        private String line;
        private String[] tokens;

        Tester(String line, String tokens[]) {
            this.line = line;
            this.tokens = tokens;
        }

        public void test(Tokenizer t) {
            List<String> test = t.tokenize(line);
            // System.out.println("line=("+line+") return="+test+":"+test.size()+" matchlen="+tokens.length);
            assertTrue(test + " <> " + tokens.length, test.size() == tokens.length);
            for (int i = 0; i < tokens.length; i++) {
                String tt = test.get(i);
                assertTrue(tt == tokens[i] || tt.equals(tokens[i]));
            }
        }
    }

    @Test
    public void testTokenizer1() {
        Tokenizer t = new Tokenizer().setSeparator(",;").setGrouping(new String[]{"'", "[]", "{}"}).setPacking(false);

        new Tester("a1", new String[]{"a1"}).test(t);
        new Tester("a1,", new String[]{"a1", ""}).test(t);
        new Tester("a1,c1,", new String[]{"a1", "c1", ""}).test(t);
        new Tester("a1,,c1", new String[]{"a1", "", "c1"}).test(t);
        new Tester("a1,b1,c1", new String[]{"a1", "b1", "c1"}).test(t);
        new Tester("'abc','def','g h i'", new String[]{"abc", "def", "g h i"}).test(t);
        new Tester(",,", new String[]{"", "", ""}).test(t);
        new Tester(";\\';abc def; ghi", new String[]{"", "'", "abc def", " ghi"}).test(t);
        new Tester("[a b c],def,{g,[h] i},jkl", new String[]{"a b c", "def", "g,[h] i", "jkl"}).test(t);

        t = new Tokenizer().setSeparator(",").setGrouping(new String[]{"'", "[]", "{}"}).setPacking(true);
        new Tester("a1", new String[]{"a1"}).test(t);
        new Tester("a1,", new String[]{"a1"}).test(t);
        new Tester("a1,c1,", new String[]{"a1", "c1"}).test(t);
        new Tester("a1,,c1", new String[]{"a1", "c1"}).test(t);
    }

    protected Tokenizer tokens;
    protected List<String> line;

    @Before
    public void setUp() throws Exception {
        tokens = new Tokenizer().setSeparator("|\t").setGrouping(new String[]{"[]", "\""}).setPacking(false);
    }

    @Test
    public void testNull() throws Exception {
        line = tokens.tokenize(null);
        assertNull(line);
        line = tokens.tokenize("   ");
        assertNull(line);
    }

    @Test
    public void testSuccess() throws Exception {
        line = tokens.tokenize("a\tb\tc");
        assertNotNull(line);
        assertEquals(3, line.size());
        assertEquals("a", line.get(0));
        assertEquals("b", line.get(1));
        assertEquals("c", line.get(2));
    }

    @Test
    public void testEmptyFields() throws Exception {
        line = tokens.tokenize("a\t\tc");
        assertNotNull(line);
        assertEquals(3, line.size());
        assertEquals("a", line.get(0));
        assertEquals("", line.get(1));
        assertEquals("c", line.get(2));
    }

    @Test
    public void testGroup1() throws Exception {
        line = tokens.tokenize("a|\"b\tb\"|c");
        assertNotNull(line);
        assertEquals(3, line.size());
        assertEquals("a", line.get(0));
        assertEquals("b\tb", line.get(1));
        assertEquals("c", line.get(2));
    }

    @Test
    public void testGroup2() throws Exception {
        line = tokens.tokenize("a|[b\tb]|c");
        assertNotNull(line);
        assertEquals(3, line.size());
        assertEquals("a", line.get(0));
        assertEquals("b\tb", line.get(1));
        assertEquals("c", line.get(2));
    }

    @Test
    public void testPack() throws Exception {
        tokens = new Tokenizer().setSeparator("\t|").setGrouping(new String[]{"[]", "\""}).setPacking(true);
        // tokens = new Tokenizer("\t", new String[] { "[]", "\"" }, true,
        // Collections.singletonMap(" | ", "\t"));
        line = tokens.tokenize("a||c");
        assertNotNull(line);
        assertEquals("line=" + line, 2, line.size());
        assertEquals("a", line.get(0));
        assertEquals("c", line.get(1));
    }
}
