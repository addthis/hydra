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
package com.addthis.hydra.data.filter.util;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.config.Configs;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

import org.junit.Assert;
import org.junit.Test;

public class AutoFieldTest {

    private static void setAndFilterBundle(Bundle bundle, BundleFilter filter) {
        BundleField a = bundle.getFormat().getField("a");
        BundleField b = bundle.getFormat().getField("b");
        bundle.setValue(a, ValueFactory.create("SANDWICH"));
        filter.filter(bundle);
        filter.filter(bundle);
        filter.filter(bundle);
        Assert.assertEquals("SANDWICH", bundle.getValue(b).toString());
    }

    @Test
    public void createAndAccess() {
        SimpleCopyFilter filter = Configs.decodeObject(SimpleCopyFilter.class, "from = a, to = b");
        Bundle bundle = new ListBundle();
        setAndFilterBundle(bundle, filter);
    }

    @Test
    public void kvBundles() {
        SimpleCopyFilter filter = Configs.decodeObject(SimpleCopyFilter.class, "from = a, to = b");
        Bundle bundle = new KVBundle();
        setAndFilterBundle(bundle, filter);
    }

    @Test
    public void changingFormats() {
        SimpleCopyFilter filter = Configs.decodeObject(SimpleCopyFilter.class, "from = a, to = b");
        Bundle bundle = new KVBundle();
        setAndFilterBundle(bundle, filter);
        bundle = new ListBundle();
        bundle.getFormat().getField("c");
        setAndFilterBundle(bundle, filter);
        bundle = new ListBundle();
        setAndFilterBundle(bundle, filter);
    }

    public static class SimpleCopyFilter extends BundleFilter {
        @FieldConfig(required = true) private AutoField from;
        @FieldConfig(required = true) private AutoField to;

        @Override public void initialize() {}

        @Override public boolean filterExec(Bundle row) {
            to.setValue(row, from.getValue(row));
            return true;
        }
    }

}