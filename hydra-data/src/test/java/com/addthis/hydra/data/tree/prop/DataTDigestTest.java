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
package com.addthis.hydra.data.tree.prop;

import com.clearspring.analytics.stream.quantile.TDigest;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataTDigestTest {

    @Test
    public void testMerge() {
        TDigest t1 = new TDigest(100);
        TDigest t2 = new TDigest(100);
        t1.add(2);
        t1.add(4);
        t1.add(6);
        t1.add(8);
        t1.add(10);
        t2.add(1);
        t2.add(3);
        t2.add(5);
        t2.add(7);
        t2.add(9);
        DataTDigest.TDigestValue v1 = new DataTDigest.TDigestValue(t1,
                DataTDigest.TDigestValue.OP.QUANTILE, 0.90);
        DataTDigest.TDigestValue v2 = new DataTDigest.TDigestValue(t2,
                DataTDigest.TDigestValue.OP.QUANTILE, 0.90);
        assertEquals(8, v1.sum(v2).asLong().getLong());
        assertEquals(8, v2.sum(v1).asLong().getLong());
    }

}
