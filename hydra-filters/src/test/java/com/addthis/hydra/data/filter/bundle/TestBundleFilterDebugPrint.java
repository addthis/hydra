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

import com.addthis.bundle.core.kvp.KVBundle;
import com.addthis.bundle.core.kvp.KVBundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBundleFilterDebugPrint extends TestBundleFilter {

    @Test
    public void debugPrintMapBundleTest() {
        // The map bundle converts everything into a string.
        BundleFilterDebugPrint bf = new BundleFilterDebugPrint().enableCacheOutput();
        TestBundleFilter.MapBundle bundle = createBundle(new String[]{"hello", "1", "dog", "food", "foo", null});
        bf.filter(bundle);
        assertEquals("{\"hello\" = \"1\" , \"foo\" = null , \"dog\" = \"food\"}", bf.getCacheOutput());
    }

    @Test
    public void debugPrintKVBundleTest() {
        BundleFilterDebugPrint bf = new BundleFilterDebugPrint().enableCacheOutput();
        KVBundleFormat bundleFormat = new KVBundleFormat();
        KVBundle bundle = new KVBundle(bundleFormat);
        bundle.setValue(bundleFormat.getField("hello"), ValueFactory.create(1));
        bundle.setValue(bundleFormat.getField("world"), ValueFactory.create(1.0));
        bundle.setValue(bundleFormat.getField("foo"), null);
        bundle.setValue(bundleFormat.getField("dog"), ValueFactory.create("food"));
        bf.filter(bundle);
        assertEquals("{\"hello\" = 1 , \"world\" = 1.0 , \"foo\" = null , \"dog\" = \"food\"}",
                bf.getCacheOutput());
    }

    @Test
    public void debugPrintKVBundleArrayTest() {
        BundleFilterDebugPrint bf = new BundleFilterDebugPrint().enableCacheOutput();
        KVBundleFormat bundleFormat = new KVBundleFormat();
        KVBundle bundle = new KVBundle(bundleFormat);
        ValueArray array = ValueFactory.createArray(3);
        array.add(ValueFactory.create(1));
        array.add(ValueFactory.create(1.0));
        array.add(null);
        bundle.setValue(bundleFormat.getField("array"), array);

        bf.filter(bundle);
        assertEquals("{\"array\" = [1 , 1.0 , null]}", bf.getCacheOutput());
    }

    @Test
    public void debugPrintKVBundleMapTest() {
        BundleFilterDebugPrint bf = new BundleFilterDebugPrint().enableCacheOutput();
        KVBundleFormat bundleFormat = new KVBundleFormat();
        KVBundle bundle = new KVBundle(bundleFormat);
        ValueMap map = ValueFactory.createMap();
        map.put("foo", ValueFactory.create(1));
        map.put("bar", ValueFactory.create(1.0));
        map.put("baz", ValueFactory.create("baz"));
        map.put("quux", null);
        bundle.setValue(bundleFormat.getField("map"), map);

        bf.filter(bundle);
        assertEquals("{\"map\" = {\"baz\" : \"baz\" , \"foo\" : 1 , \"bar\" : 1.0 , \"quux\" : null}}",
                bf.getCacheOutput());
    }


}
