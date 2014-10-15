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
import static org.junit.Assert.assertTrue;

public class TestBundleFilterRandomField {

    @Test
    public void fieldTest() {
        BundleFilterRandomField bfrf = new BundleFilterRandomField(new String[]{"f0", "f1"}, "out0");
        MapBundle bundle = MapBundle.createBundle(new String[]{"f0", "foo", "f1", "bar"});
        bfrf.filter(bundle);
        assertTrue(bundle.get("out0").equals("foo") || bundle.get("out0").equals("bar"));
    }

    @Test
    public void fieldTestNull() {
        BundleFilterRandomField bfrf = new BundleFilterRandomField(new String[]{"f0", "f1"}, "out0");
        MapBundle bundle = MapBundle.createBundle(new String[]{"f0", null, "f1", "bar"});
        bfrf.filter(bundle);
        assertEquals("bar", bundle.get("out0"));
    }

}
