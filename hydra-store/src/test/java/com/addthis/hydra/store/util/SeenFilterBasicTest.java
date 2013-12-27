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
package com.addthis.hydra.store.util;

import com.addthis.basis.test.SlowTest;

import com.addthis.codec.CodecJSON;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SlowTest.class)
public class SeenFilterBasicTest {

    @Test
    public void basicTest() throws Exception {
        checkSeenFilter(genFilter());
    }

    @Test
    public void encodeTest() throws Exception {
        String encoded = CodecJSON.encodeString(genFilter());
        checkSeenFilter(CodecJSON.decodeString(new SeenFilterBasic<String>(), encoded));
    }

    private SeenFilterBasic<String> genFilter() {
        SeenFilterBasic<String> filter = new SeenFilterBasic<String>(20000, 4, 4);
        for (int i = 0; i < 1000; i++) {
            filter.setSeen(i + "=" + i);
        }
        return filter;
    }

    private void checkSeenFilter(SeenFilterBasic<String> filter) {
        for (int i = 0; i < 1000; i++) {
            assertTrue("fail seen @ i = " + i, filter.getSeen(i + "=" + i));
        }
        int err = 0;
        for (int i = 1000; i < 2000; i++) {
            if (filter.getSeen(i + "=" + i)) {
                err++;
            }
        }
        assertTrue("fail !seen with err = " + err, err <= 10);
    }
}
