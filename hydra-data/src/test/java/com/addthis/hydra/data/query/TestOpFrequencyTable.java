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

import com.addthis.hydra.data.query.op.OpFrequencyTable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestOpFrequencyTable extends TestOp {

    @Test
    public void testEmpty() throws Exception {
        doOpTest(parse(""), "ftable=0,1:2,3:3", parse(""));
    }

    @Test
    public void testOneRow() throws Exception {
        doOpTest(parse("A 1 1"), "ftable=0:1,2:0.5", parse("A 1"));
        doOpTest(parse("A 1 1"), "ftable=0:1,2:total,0.5", parse("A 1 1"));
    }


    @Test
    public void testMultiRow() throws Exception {
        doOpTest(parse("A B 1 1 | C D 3 30 | C D 20 1"), "ftable=0,1:2,3:0.5,0.75", parse("A B 1 1 | C D 3 3"));
        doOpTest(parse("q w 1 1 | q w  2 20 |  C D 3 30 | C D 20 1"), "ftable=0,1:2,3:0.5,0.75", parse("q w 2 2 | C D 3 3"));
        doOpTest(parse("q w 1 1 | q w  2 20 |  C D 3 30 | C D 20 1 | z z 5 5"), "ftable=0,1:2,3:0.5,0.75", parse("q w 2 2 | C D 3 3 | z z 5 5"));
        doOpTest(parse("a a 5 5 | q w 1 1 | q w  2 20 |  C D 3 30 | C D 20 1 | z z 5 5"), "ftable=0,1:2,3:0.5,0.75", parse("a a 5 5 | q w 2 2 | C D 3 3 | z z 5 5"));
        doOpTest(parse("a a 5 5 | q w 1 1 | q x  2 20 |  C e 3 30 | C f 20 1 | z z 5 5"), "ftable=0,1:2,3:0.5,0.75", parse("a a 5 5 | q w 1 1 | q x 2 2 | C e 3 3 | C f 20 20 | z z 5 5"));
    }

    @Test
    public void testExtendedAppend() throws Exception {
        doOpTest(parse("A B 1 1"), "ftable=0,1:2,3:total,0.25,0.5,0.75,0.80", parse("A B 1 1 1 1 1"));
    }


    // Ftable Tests
    @Test
    public void ftableTestSingle() throws Exception {
        OpFrequencyTable.FTable ftable = new OpFrequencyTable.FTable();
        ftable.update(1L, 1L);
        assertEquals(1L, ftable.getNearestPercentile(0.5));

        ftable.update(2L, 1L);
        ftable.update(3L, 1L);
        assertEquals(2L, ftable.getNearestPercentile(0.5));
    }

    @Test
    public void ftableTestMulti() throws Exception {
        OpFrequencyTable.FTable ftable = new OpFrequencyTable.FTable();
        ftable.update(1L, 1000L);
        assertEquals(1L, ftable.getNearestPercentile(0.99));

        ftable.update(2L, 1L);
        ftable.update(3L, 1L);
        assertEquals(1L, ftable.getNearestPercentile(0.5));
        ftable.update(10L, 10000L);
        assertEquals(1L, ftable.getNearestPercentile(0.01));
        assertEquals(10L, ftable.getNearestPercentile(0.5));
        ftable.update(1L, 1000000L);
        assertEquals(1L, ftable.getNearestPercentile(0.5));
    }


}