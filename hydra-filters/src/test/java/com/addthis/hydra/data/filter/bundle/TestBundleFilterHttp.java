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

import com.addthis.basis.test.SlowTest;

import com.addthis.bundle.util.map.MapBundle;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;


@Category(SlowTest.class)
public class TestBundleFilterHttp {

    @Test
    public void fieldTest() {
        BundleFilterHttp bfh = BundleFilterHttp.create(BundleFilterTemplate.create(new String[]{"http://", "{{domain}}", "/", "{{path}}"}, "url"), "content");
        MapBundle bundle = MapBundle.createBundle(new String[]{"domain", "addthis.com", "path", ""});
        bfh.filter(bundle);
        assertTrue(bundle.get("content") != null);
    }
}
