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
package com.addthis.hydra.data.query;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestQueryElementNode {

    @Test
    public void extractDefaultValue() {
        QueryElementNode node = new QueryElementNode();
        String output = node.extractDefaultValue("+");
        assertEquals("+", output);
        assertEquals(null, node.defaultValue);
        assertEquals(0, node.defaultHits);
        node = new QueryElementNode();
        output = node.extractDefaultValue("+[abc]");
        assertEquals("+", output);
        assertEquals("abc", node.defaultValue);
        assertEquals(0, node.defaultHits);
        node = new QueryElementNode();
        output = node.extractDefaultValue("+%[abc]foo=bar");
        assertEquals("+%foo=bar", output);
        assertEquals("abc", node.defaultValue);
        assertEquals(0, node.defaultHits);
        node = new QueryElementNode();
        output = node.extractDefaultValue("+[abc]d,e,f");
        assertEquals("+d,e,f", output);
        assertEquals("abc", node.defaultValue);
        assertEquals(0, node.defaultHits);
        node = new QueryElementNode();
        output = node.extractDefaultValue("+[abc|15]d,e,f");
        assertEquals("+d,e,f", output);
        assertEquals("abc", node.defaultValue);
        assertEquals(15, node.defaultHits);
    }
}
