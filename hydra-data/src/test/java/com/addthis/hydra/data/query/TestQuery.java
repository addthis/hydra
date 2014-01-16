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

import org.junit.Assert;
import org.junit.Test;

public class TestQuery {

    @Test
    public void testCompact() {
        String path = "+:+hits,+nodes$+foo=123/+/++123/+%top=hit/a,b,c/|foo/|+bar/*/+%goo/(1-5)+";
        Query q = new Query("job", new String[] { path }, null);
        Assert.assertEquals(path, q.getPathString(q.getQueryPaths().get(0)));
    }

    @Test
    public void pipeline() {
        String path = "+:+hits,+nodes$+foo=123/+/++123/+%top=hit/a,b,c/|foo/|+bar/*/+%goo/(1-5)+";
        String[] ops = {"sort"};
        Query q = new Query("job", new String[] { path }, ops);
        Query subQ = q.createPipelinedQuery();
        System.out.println(subQ.toString());
    }
}
