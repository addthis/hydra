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

import com.addthis.bundle.util.CachingField;
import com.addthis.bundle.util.map.MapBundle;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBundleFilterNot {

    @Test
    public void fieldTest() {
        BundleFilterNot bff = new BundleFilterNot(new CachingField("foo"));
        MapBundle bundle = MapBundle.createBundle(new String[]{"dog", "food"});
        assertTrue(bff.filter(bundle));
    }

    @Test
    public void fieldTest_False() {
        BundleFilterNot bff = new BundleFilterNot(new CachingField("foo"));
        MapBundle bundle = MapBundle.createBundle(new String[]{"dog", "food", "foo", "bar"});
        assertFalse(bff.filter(bundle));
    }


}
