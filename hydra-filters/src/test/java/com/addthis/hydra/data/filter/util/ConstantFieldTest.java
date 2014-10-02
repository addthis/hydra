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

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.codec.config.Configs;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.filter.bundle.BundleFilterField;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConstantFieldTest extends AutoFieldTest {

    @Override protected BundleFilter createSampleFilter() throws IOException {
        return Configs.decodeObject(SimpleCopyFilter.class, "from.const {codec = good}, to = [b, sub-b]");
    }

    @Override protected void setAndFilterBundle(Bundle bundle, BundleFilter filter) {
        BundleField b = bundle.getFormat().getField("b");
        bundle.setValue(b, ValueFactory.createMap());
        filter.filter(bundle);
        filter.filter(bundle);
        filter.filter(bundle);
        ValueMap<?> subB = bundle.getValue(b).asMap();
        assertEquals("good", subB.get("sub-b").asMap().get("codec").toString());
    }

    @Test
    public void setKeyFromValue() throws Exception {
        BundleFilter filter = Configs.decodeObject(SimpleCopyFilter.class, "from.const: foobar, to: MAP/mykey");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        b.setValue(b.getFormat().getField("MAP"), map);
        b.setValue(b.getFormat().getField("MYFIELD"), ValueFactory.create("blah"));
        filter.filter(b);
        assertEquals("foobar", map.get("mykey").asNative());
    }

    @Test
    public void replaceKeyFromValue() throws Exception {
        BundleFilter filter = Configs.decodeObject(BundleFilterField.class, "from.const:foobar, to:MAP.mykey");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        map.put("mykey",ValueFactory.create("abc"));
        b.setValue(b.getFormat().getField("MAP"), map);
        b.setValue(b.getFormat().getField("MYFIELD"), ValueFactory.create("blah"));
        filter.filter(b);
        assertEquals("foobar", map.get("mykey").asNative());
    }
}