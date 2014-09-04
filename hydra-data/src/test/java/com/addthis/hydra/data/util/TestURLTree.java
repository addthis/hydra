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

import java.util.Set;

import com.google.common.collect.Sets;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestURLTree {

    @Test
    public void testTree() {
        URLTree urlTree = new URLTree();
        urlTree.addURLPath("www.foo.com/1/2/3", 1);
        urlTree.addURLPath("www.foo.com/1/2/3/", 1);
        urlTree.addURLPath("www.foo.com/1/2/4", 1);
        urlTree.addURLPath("www.foo.com/1/2/3/4", 1);
        urlTree.addURLPath("www.foo.com/1/2/3", 1);
        urlTree.addURLPath("www.foo.com/1/2/3/5", 1);
        urlTree.addURLPath("www.foo.com/1/2/3/5/", 1);
        urlTree.addURLPath("www.foo.com", 1);

        Set<UrlTreeObject.TreeValue> branches = Sets.newHashSet(urlTree.getBranches("/"));
        Set<UrlTreeObject.TreeValue> expected = Sets.newHashSet(
                new UrlTreeObject.TreeValue("www.foo.com/1/2/3/4", 1.0),
                new UrlTreeObject.TreeValue("www.foo.com/1/2/3/5/", 2.0),
                new UrlTreeObject.TreeValue("www.foo.com/1/2/4", 1.0)
        );
        assertEquals(expected, branches);
    }
}
