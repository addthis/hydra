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

import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.config.Configs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestBundleFilterForeach {

    @Test
    public void foreachUsingIndex() throws Exception {
        BundleFilterForeach filter = Configs.decodeObject(BundleFilterForeach.class,
                                                          "filter: " +
                                                          "[{op:field, from:SOURCE, to:ELEMENT, " +
                                                          "filter:{op:index, index.field:INDEX }}, " +
                                                          "{op:concat, in:[ OUTPUT, ELEMENT ], " +
                                                          "out: OUTPUT, join: \":\"}]," +
                                                          "source:SOURCE ," +
                                                          "index:INDEX,");
        ValueArray array = ValueFactory.createArray(3);
        array.add(ValueFactory.create("foo"));
        array.add(ValueFactory.create("bar"));
        array.add(ValueFactory.create("baz"));
        AutoField source = AutoField.newAutoField("SOURCE");
        AutoField output = AutoField.newAutoField("OUTPUT");
        ListBundle bundle = new ListBundle();
        source.setValue(bundle, array);
        output.setValue(bundle, ValueFactory.create(""));
        assertTrue(filter.filter(bundle));
        assertEquals(":foo:bar:baz", output.getString(bundle).get());
    }

    @Test
    public void foreachUsingValue() throws Exception {
        BundleFilterForeach filter = Configs.decodeObject(BundleFilterForeach.class,
                                                          "filter: {op:concat, in:[ OUTPUT, ELEMENT ], " +
                                                          "out: OUTPUT, join: \":\"}," +
                                                          "source:SOURCE ," +
                                                          "value:ELEMENT,");
        ValueArray array = ValueFactory.createArray(3);
        array.add(ValueFactory.create("foo"));
        array.add(ValueFactory.create("bar"));
        array.add(ValueFactory.create("baz"));
        AutoField source = AutoField.newAutoField("SOURCE");
        AutoField output = AutoField.newAutoField("OUTPUT");
        ListBundle bundle = new ListBundle();
        source.setValue(bundle, array);
        output.setValue(bundle, ValueFactory.create(""));
        assertTrue(filter.filter(bundle));
        assertEquals(":foo:bar:baz", output.getString(bundle).get());
    }
}
