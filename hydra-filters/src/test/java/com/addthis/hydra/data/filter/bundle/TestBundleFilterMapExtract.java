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

import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestBundleFilterMapExtract {

    @Test
    public void fieldTest() {
        BundleFilterMapExtract filter = new BundleFilterMapExtract();
        KVBundle bundle = new KVBundle();
        BundleFormat format = bundle.getFormat();
        ValueMap map = ValueFactory.createMap();
        map.put("foo", ValueFactory.create("a"));
        map.put("bar", ValueFactory.create("b"));
        map.put("baz", ValueFactory.create("c"));
        bundle.setValue(format.getField("map"), map);
        BundleFilterMapExtract.XMap xmap = new BundleFilterMapExtract.XMap().setFrom("foo");
        BundleFilterMapExtract.XMap[] maps = new BundleFilterMapExtract.XMap[1];
        maps[0] = xmap;
        filter = filter.setField(AutoField.newAutoField("map")).setMap(maps);
        assertFalse(format.hasField("foo"));
        filter.filter(bundle);
        assertTrue(format.hasField("foo"));
        assertEquals("a", bundle.getValue(format.getField("foo")).asString().toString());
    }

    @Test
    public void fieldTestMissing() {
        BundleFilterMapExtract filter = new BundleFilterMapExtract();
        KVBundle bundle = new KVBundle();
        BundleFormat format = bundle.getFormat();
        ValueMap map = ValueFactory.createMap();
        map.put("foo", ValueFactory.create("a"));
        map.put("bar", ValueFactory.create("b"));
        map.put("baz", ValueFactory.create("c"));
        bundle.setValue(format.getField("map"), map);
        BundleFilterMapExtract.XMap xmap = new BundleFilterMapExtract.XMap().setFrom("quux");
        BundleFilterMapExtract.XMap[] maps = new BundleFilterMapExtract.XMap[1];
        maps[0] = xmap;
        filter = filter.setField(AutoField.newAutoField("map")).setMap(maps);
        assertFalse(format.hasField("quux"));
        filter.filter(bundle);
        assertFalse(format.hasField("quux"));
    }

    @Test
    public void fieldTestNullValue() {
        BundleFilterMapExtract filter = new BundleFilterMapExtract();
        KVBundle bundle = new KVBundle();
        BundleFormat format = bundle.getFormat();
        ValueMap map = ValueFactory.createMap();
        map.put("foo", ValueFactory.create("a"));
        map.put("bar", ValueFactory.create("b"));
        map.put("quux", null);
        bundle.setValue(format.getField("map"), map);
        BundleFilterMapExtract.XMap xmap = new BundleFilterMapExtract.XMap().setFrom("quux");
        BundleFilterMapExtract.XMap[] maps = new BundleFilterMapExtract.XMap[1];
        maps[0] = xmap;
        filter = filter.setField(AutoField.newAutoField("map")).setMap(maps);
        assertFalse(format.hasField("quux"));
        filter.filter(bundle);
        assertFalse(format.hasField("quux"));
    }

    @Test
    public void indirectionTest() {
        BundleFilterMapExtract filter = new BundleFilterMapExtract();
        KVBundle bundle = new KVBundle();
        BundleFormat format = bundle.getFormat();
        ValueMap map = ValueFactory.createMap();
        map.put("foo", ValueFactory.create("a"));
        map.put("bar", ValueFactory.create("b"));
        map.put("baz", ValueFactory.create("c"));
        map.put("field1", ValueFactory.create("d"));
        bundle.setValue(format.getField("map"), map);
        bundle.setValue(format.getField("field1"), ValueFactory.create("foo"));
        BundleFilterMapExtract.XMap xmap = new BundleFilterMapExtract.XMap().setFrom("field1").setIndirection(1);
        BundleFilterMapExtract.XMap[] maps = new BundleFilterMapExtract.XMap[1];
        maps[0] = xmap;
        filter = filter.setField(AutoField.newAutoField("map")).setMap(maps);
        filter.filter(bundle);
        assertEquals("a", bundle.getValue(format.getField("field1")).asString().toString());
    }

    @Test
    public void indirectionMissingTest() {
        BundleFilterMapExtract filter = new BundleFilterMapExtract();
        KVBundle bundle = new KVBundle();
        BundleFormat format = bundle.getFormat();
        ValueMap map = ValueFactory.createMap();
        map.put("foo", ValueFactory.create("a"));
        map.put("bar", ValueFactory.create("b"));
        map.put("baz", ValueFactory.create("c"));
        map.put("field1", ValueFactory.create("d"));
        bundle.setValue(format.getField("map"), map);
        bundle.setValue(format.getField("field1"), ValueFactory.create("quux"));
        BundleFilterMapExtract.XMap xmap = new BundleFilterMapExtract.XMap().setFrom("field1").setIndirection(1);
        BundleFilterMapExtract.XMap[] maps = new BundleFilterMapExtract.XMap[1];
        maps[0] = xmap;
        filter = filter.setField(AutoField.newAutoField("map")).setMap(maps);
        filter.filter(bundle);
        assertEquals("quux", bundle.getValue(format.getField("field1")).asString().toString());
    }

}
