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
package com.addthis.hydra.data.tree.prop;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.config.Configs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DataLimitCMSTest {

    @Test
    public void lowerBound() throws Exception {
        ListBundleFormat format = new ListBundleFormat();
        DataLimitCMS dataCountMinSketch = new DataLimitCMS(10, 100_000);
        dataCountMinSketch.add("a", 5);
        dataCountMinSketch.add("b", 2);
        dataCountMinSketch.add("a", 7);
        assertEquals(12l, dataCountMinSketch.getValue("val(a)").asLong().asNative().longValue());
        assertEquals(2l, dataCountMinSketch.getValue("val(b)").asLong().asNative().longValue());
        assertEquals(0l, dataCountMinSketch.getValue("val(c)").asLong().asNative().longValue());
        AutoField field = AutoField.newAutoField("field");
        DataLimitCMS.Config config = Configs.decodeObject(DataLimitCMS.Config.class,
                                                          "key: field, limit: 5, upper:false");
        Bundle bundle = format.createBundle();
        field.setValue(bundle, ValueFactory.create("b"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNull(field.getValue(bundle));
        field.setValue(bundle, ValueFactory.create("b"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNull(field.getValue(bundle));
        field.setValue(bundle, ValueFactory.create("b"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNull(field.getValue(bundle));
        field.setValue(bundle, ValueFactory.create("b"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNotNull(field.getValue(bundle));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNotNull(field.getValue(bundle));
        assertEquals(5l, dataCountMinSketch.getValue("val(b)").asLong().asNative().longValue());
    }


    @Test
    public void upperBound() throws Exception {
        ListBundleFormat format = new ListBundleFormat();
        DataLimitCMS dataCountMinSketch = new DataLimitCMS(10, 100_000);
        dataCountMinSketch.add("a", 5);
        dataCountMinSketch.add("b", 2);
        dataCountMinSketch.add("a", 7);
        assertEquals(12l, dataCountMinSketch.getValue("val(a)").asLong().asNative().longValue());
        assertEquals(2l, dataCountMinSketch.getValue("val(b)").asLong().asNative().longValue());
        assertEquals(0l, dataCountMinSketch.getValue("val(c)").asLong().asNative().longValue());
        AutoField field = AutoField.newAutoField("field");
        DataLimitCMS.Config config = Configs.decodeObject(DataLimitCMS.Config.class,
                                                               "key: field, limit: 5, upper:true");
        Bundle bundle = format.createBundle();
        field.setValue(bundle, ValueFactory.create("a"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNull(field.getValue(bundle));
        field.setValue(bundle, ValueFactory.create("a"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNull(field.getValue(bundle));
        assertEquals(12l, dataCountMinSketch.getValue("val(a)").asLong().asNative().longValue());
        field.setValue(bundle, ValueFactory.create("b"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNotNull(field.getValue(bundle));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNotNull(field.getValue(bundle));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNotNull(field.getValue(bundle));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNull(field.getValue(bundle));
        field.setValue(bundle, ValueFactory.create("b"));
        dataCountMinSketch.updateChildData(bundle, config);
        assertNull(field.getValue(bundle));
        assertEquals(5l, dataCountMinSketch.getValue("val(b)").asLong().asNative().longValue());
    }

}
