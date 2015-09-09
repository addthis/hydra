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

import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.codec.config.Configs;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DataCopyTest {

    private static DataTreeNodeUpdater generateUpdater(Bundle bundle) {
        return new DataTreeNodeUpdater() {
            @Override public Bundle getBundle() {
                return bundle;
            }

            @Override public int getCountValue() {
                return 0;
            }

            @Override public long getAssignmentValue() {
                return 0;
            }
        };
    }

    @Test
    public void copyValue() throws Exception {
        AutoField foo = AutoField.newAutoField("foo");
        DataCopy.Config config = Configs.decodeObject(DataCopy.Config.class, "key: {foo: foo}");
        Bundle bundle = new ListBundle();
        DataCopy dataCopy = config.newInstance();
        foo.setValue(bundle, ValueFactory.create("bar"));
        assertNull(dataCopy.getValue("foo"));
        dataCopy.updateChildData(generateUpdater(bundle), null, config);
        assertEquals(ValueFactory.create("bar"), dataCopy.getValue("foo"));
    }

    @Test
    public void copyMap() throws Exception {
        AutoField foo = AutoField.newAutoField("foo");
        DataCopy.Config config = Configs.decodeObject(DataCopy.Config.class, "map: foo");
        Bundle bundle = new ListBundle();
        DataCopy dataCopy = config.newInstance();
        ValueMap valueMap = ValueFactory.createMap();
        valueMap.put("bar", ValueFactory.create("baz"));
        valueMap.put("hello", ValueFactory.create("world"));
        foo.setValue(bundle, valueMap);
        assertNull(dataCopy.getValue("foo"));
        assertEquals(0, dataCopy.getNodes(null, "").size());
        dataCopy.updateChildData(generateUpdater(bundle), null, config);
        List<DataTreeNode> nodes = dataCopy.getNodes(null, "");
        assertEquals(2, nodes.size());
        DataTreeNode bar = nodes.get(0);
        DataTreeNode hello = nodes.get(1);
        if (bar.getName().compareTo(hello.getName()) > 0) {
            DataTreeNode temp = bar;
            bar = hello;
            hello = temp;
        }
        assertEquals("bar", bar.getName());
        assertEquals("hello", hello.getName());
        assertNotNull(bar.getNode("baz"));
        assertNotNull(hello.getNode("world"));
    }

}
