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

import org.junit.Test;

public class BundleFilterTestTest {

    @Test public void allTrue() {
        Bundle bundle = new ListBundle();
        BundleFilterTest filter = Configs.decodeObject(
                BundleFilterTest.class, "test = [], onTrue = [], onFalse = []");
        filter.filterExec(bundle);
    }
}
