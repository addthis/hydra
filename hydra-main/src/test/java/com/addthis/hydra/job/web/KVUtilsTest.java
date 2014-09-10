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
package com.addthis.hydra.job.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.addthis.basis.kv.KVPairs;

import org.junit.Before;
import org.junit.Test;

public class KVUtilsTest {

    private KVPairs kv;

    @Before
    public void setUp() {
        kv = new KVPairs();
    }

    @Test
    public void getValue() {
        kv.add("foo", "bar");
        kv.add("toto", "tata");
        assertEquals("primary key", "bar", KVUtils.getValue(kv, "default", "foo", "toto"));
        assertEquals("secondary key", "tata", KVUtils.getValue(kv, "default", "bogus", "toto"));
        assertEquals("default value", "default", KVUtils.getValue(kv, "default", "bogus", "bogus_too"));
    }

    @Test
    public void getLongValue() {
        kv.add("foo", "1");
        kv.add("toto", "2");
        assertEquals("primary key", new Long(1), KVUtils.getLongValue(kv, 0L, "foo", "toto"));
        assertEquals("secondary key", new Long(2), KVUtils.getLongValue(kv, 0L, "bogus", "toto"));
        assertEquals("default value", new Long(0), KVUtils.getLongValue(kv, 0L, "bogus", "bogus_too"));
    }

    @Test
    public void getBooleanValue() {
        kv.add("foo", "true");
        kv.add("toto", "0");
        assertTrue("primary key", KVUtils.getBooleanValue(kv, false, "foo", "toto"));
        assertFalse("secondary key", KVUtils.getBooleanValue(kv, true, "bogus", "toto"));
        assertTrue("default value", KVUtils.getBooleanValue(kv, true, "bogus", "bogus_too"));
    }

}
