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

import com.addthis.hydra.common.hash.PluggableHashFunction;

import org.junit.Test;

public class TestOpNumber extends TestOp {

    @Test
    public void testNumber() throws Exception {
        doOpTest(new DataTableHelper().tr(), "num=n1,n2,add,v-1,set", new DataTableHelper().tr().td("3"));
        doOpTest(new DataTableHelper().tr(), "num=n1,n4,sub,v-1,set", new DataTableHelper().tr().td("-3"));
        doOpTest(new DataTableHelper().tr(), "num=n2,n5,swap,sub,v-1,set", new DataTableHelper().tr().td("3"));
        doOpTest(new DataTableHelper().tr(), "num=n2,n5,dup,mult,sub,v-1,set", new DataTableHelper().tr().td("-23"));
        doOpTest(new DataTableHelper().tr(), "num=n4,n2,div,v-1,set", new DataTableHelper().tr().td("2"));
        doOpTest(new DataTableHelper().tr(), "num=n3,n2,ddiv,v-1,set", new DataTableHelper().tr().td("1.5"));
        doOpTest(new DataTableHelper().tr(), "num=n3.0,tob,btof,v-1,set", new DataTableHelper().tr().td("3.0"));
        DataTableHelper s1 = new DataTableHelper().
                tr().td("A", "1", "art").
                tr().td("B", "2", "bot").
                tr().td("C", "3", "cog").
                tr().td("D", "4", "din");
        doOpTest(s1, "num=n4,c1,mult,v2,set", parse("A 1 4|B 2 8|C 3 12|D 4 16"));
        doOpTest(s1, "num=n1.5,c1,dmult,v2,set", parse("A 1 1.5|B 2 3.0|C 3 4.5|D 4 6.0"));
        doOpTest(parse("A 1 art|B 10 bot|C 100 cog|D 1000 din"), "num=c1,log,v2,set", parse("A 1 0.0|B 10 1.0|C 100 2.0|D 1000 3.0"));
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "num=n5,c1,rem,v2,set", parse("A 1 0|B 2 1|C 3 2|D 4 1"));
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "num=c1,n1,gt,c1,n3,lteq", parse("B 2 bot|C 3 cog"));
        doOpTest(parse("A 1 art|B 2 bot|C 3 cog|D 4 din"), "num=c1,n4,lt,c1,n2,gteq", parse("B 2 bot|C 3 cog"));
        doOpTest(parse("1 3 art|2 3 bot|3 3 cog|3 4 din"), "num=c0,n3,eq,n3,c1,eq", parse("3 3 cog"));
    }

    @Test
    public void testMinMaxIf() throws Exception {
        doOpTest(parse("0|5|6|0"), "num=c0,v1,maxif,v0,set", parse("0|1|1|0"));
        doOpTest(parse("5|2|3|5"), "num=c0,v4,minif,v0,set", parse("5|4|4|5"));
    }

    @Test
    public void testHashOperation() throws Exception {
        String inputString = "somethingToHash";
        int count = 123;
        String inputTable = inputString + " " + count;
        String expected = inputTable + " " + Integer.toString(123 + PluggableHashFunction.hash(inputString));
        doOpTest(parse(inputTable), "num=h0,c1,add,v2,set", parse(expected));

    }
}

