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

import com.addthis.basis.util.LessBytes;

import org.junit.Test;

public class TestOpString extends TestOp {

    @Test
    public void testSort() throws Exception {
        doOpTest(parse(""), "str=c1,c2,cat", parse(""));
        doOpTest(parse(" "), "str=vhi,vho,cat,v-1,set", parse("hiho"));
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "str=" + LessBytes.urlencode("c0,c1,+,v2,="), parse("A 1 A1|B 2 B2|C 3 C3|D 4 D4"));
        doOpTest(parse("k1:v1|k2:v2|k3:v3|"), "str=c0,v:,v0,split,v0,set", parse("k1|k2|k3"));
        doOpTest(parse("k1:v1|k2:v2|k3:v3|"), "str=c0,v1,v-1,range,v0,set", parse("1:v|2:v|3:v"));
        doOpTest(parse("abc,def"), "str=c0,v,,v0,split,v1,set", parse("abc,def abc"));
    }

    @Test
    public void testEquality() throws Exception {
        doOpTest(parse("v1 v1|v2 v2|v3 v4"), "str=c0,c1,eq", parse("v1 v1|v2 v2"));
        doOpTest(parse("v1 v1|v2 v2|v3 v4"), "str=c0,c1,neq", parse("v3 v4"));
    }
}
