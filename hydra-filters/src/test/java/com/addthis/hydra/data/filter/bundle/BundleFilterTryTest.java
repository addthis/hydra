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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.codec.config.Configs;

import org.junit.Assert;
import org.junit.Test;

public class BundleFilterTryTest {

    @Test public void failThenPass() {
        Bundle bundle = new ListBundle();
        BundleFilterTry filter = (BundleFilterTry) Configs.decodeObject(
                BundleFilter.class, "try: {fail {}}, except: {true {}}");
        Assert.assertTrue(filter.filter(bundle));
    }

    @Test public void failThenFail() {
        Bundle bundle = new ListBundle();
        BundleFilterTry filter = (BundleFilterTry) Configs.decodeObject(
                BundleFilter.class, "try: {fail {}}, except: {fail {}}");
        Assert.assertFalse(filter.filter(bundle));
    }

    @Test public void passThenFail() {
        Bundle bundle = new ListBundle();
        BundleFilterTry filter = (BundleFilterTry) Configs.decodeObject(
                BundleFilter.class, "try: {true {}}, except: {fail {}}");
        Assert.assertTrue(filter.filter(bundle));
    }

}