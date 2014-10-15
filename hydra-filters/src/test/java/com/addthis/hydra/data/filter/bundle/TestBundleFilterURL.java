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
package com.addthis.hydra.data.filter.bundle;

import com.addthis.bundle.util.map.MapBundle;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBundleFilterURL {

    @Test
    public void testSetHost() {
        BundleFilterURL filterURL = new BundleFilterURL().setField("input").setHost("host");
        MapBundle bundle = MapBundle.createBundle(new String[]{"input", "http://www.neuron.com", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("www.neuron.com", bundle.get("host"));
    }

    @Test
    public void testTopPrivateDomain() {
        BundleFilterURL filterURL = new BundleFilterURL().setField("input").setHost("host").setTopPrivateDomain(true);
        MapBundle bundle = MapBundle.createBundle(new String[]{"input", "http://a.b.c.d.addthis.com", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("addthis.com", bundle.get("host"));
        bundle = MapBundle.createBundle(new String[]{"input", "http://addthis.com", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("addthis.com", bundle.get("host"));
        bundle = MapBundle.createBundle(new String[]{"input", "http://a.b.c.d.addthis.co.uk", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("addthis.co.uk", bundle.get("host"));
        bundle = MapBundle.createBundle(new String[]{"input", "http://foobar", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("foobar", bundle.get("host"));
        bundle = MapBundle.createBundle(new String[]{"input", "http://127.0.0.1", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("127.0.0.1", bundle.get("host"));
        bundle = MapBundle.createBundle(new String[]{"input", "http://", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("", bundle.get("host"));
    }

    @Test
    public void testBaseDomain() {
        BundleFilterURL filterURL = new BundleFilterURL().setField("input").setHost("host").setBaseDomain(true);
        MapBundle bundle = MapBundle.createBundle(new String[]{"input", "http://www.neuron.com", "host", ""});
        assertTrue(filterURL.filter(bundle));
        assertEquals("neuron.com", bundle.get("host"));
    }

    @Test
    public void testFixProto() {
        BundleFilterURL filterURL = new BundleFilterURL().setField("input").setHost("host");
        MapBundle bundle = MapBundle.createBundle(new String[]{"input", "www.neuron.com", "host", ""});
        assertFalse(filterURL.filter(bundle));
        filterURL.setFixProto(true);
        assertTrue(filterURL.filter(bundle));
        assertEquals("www.neuron.com", bundle.get("host"));
    }

    @Test
    public void testSetHostNormal() {
        String[] testInput =
                {"ffff7.com", "www2.ffff7.com", "www.ffff7.com", "www1.ffff7.com",
                 "ffff.tv", "www2.ffff.tv", "www.ffff.tv", "www1.ffff.tv",
                 "www.ffff.co", "www2.ffff.co", "www1.ffff.co",
                 "ffff3.net", "www2.ffff3.net", "www.ffff3.net",
                 "ab.com", "www8.ab.com", "www.ab.com",
                 "boom.edu", "www1.boom.edu", "www.boom.edu",
                 "www.wlw.com", "wlw.com",
                 "www.zyfoobar.edu", "zyfoobar.edu"};
        String[] expectedOutput =
                {"ffff7.com", "ffff7.com", "ffff7.com", "ffff7.com",
                 "ffff.tv", "ffff.tv", "ffff.tv", "ffff.tv",
                 "ffff.co", "ffff.co", "ffff.co",
                 "ffff3.net", "ffff3.net", "ffff3.net",
                 "ab.com", "ab.com", "ab.com",
                 "boom.edu", "boom.edu", "boom.edu",
                 "wlw.com", "wlw.com",
                 "zyfoobar.edu", "zyfoobar.edu"};

        assertEquals(testInput.length, expectedOutput.length);
        for (int i = 0; i < testInput.length; i++) {
            BundleFilterURL filterURL = new BundleFilterURL().setField("input").setHostNormal("host");
            String[] input = new String[]{"input", null, "host", ""};
            input[1] = "http://" + testInput[i];
            MapBundle bundle = MapBundle.createBundle(input);
            assertTrue(filterURL.filter(bundle));
            assertEquals(expectedOutput[i], bundle.get("host"));
        }
    }

}
