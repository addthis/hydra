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
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.codec.config.Configs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBundleFilterMapPut {
    @Test
    public void setKey() throws Exception {
        BundleFilterMapPut filter = Configs.decodeObject(BundleFilterMapPut.class, "key:mykey,field:MYFIELD,map:MAP");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        b.setValue(b.getFormat().getField("MAP"), map);
        b.setValue(b.getFormat().getField("MYFIELD"), ValueFactory.create("foobar"));
        filter.filter(b);
        assertEquals("foobar", map.get("mykey").asNative());
    }

    @Test
    public void replaceKey() throws Exception {
        BundleFilterMapPut filter = Configs.decodeObject(BundleFilterMapPut.class, "key:mykey,field:MYFIELD,map:MAP");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        map.put("mykey",ValueFactory.create("abc"));
        b.setValue(b.getFormat().getField("MAP"), map);
        b.setValue(b.getFormat().getField("MYFIELD"), ValueFactory.create("foobar"));
        filter.filter(b);
        assertEquals("foobar", map.get("mykey").asNative());
    }
    
    @Test
    public void setKeyFromValue() throws Exception {
        BundleFilterMapPut filter = Configs.decodeObject(BundleFilterMapPut.class, "key:mykey,value:foobar,map:MAP");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        b.setValue(b.getFormat().getField("MAP"), map);
        b.setValue(b.getFormat().getField("MYFIELD"), ValueFactory.create("blah"));
        filter.filter(b);
        assertEquals("foobar", map.get("mykey").asNative());
    }

    @Test
    public void replaceKeyFromValue() throws Exception {
        BundleFilterMapPut filter = Configs.decodeObject(BundleFilterMapPut.class, "key:mykey,value:foobar,map:MAP");
        Bundle b = new ListBundle();
        ValueMap<String> map = ValueFactory.createMap();
        map.put("mykey",ValueFactory.create("abc"));
        b.setValue(b.getFormat().getField("MAP"), map);
        b.setValue(b.getFormat().getField("MYFIELD"), ValueFactory.create("blah"));
        filter.filter(b);
        assertEquals("foobar", map.get("mykey").asNative());
    }
    
    @Test(expected = Exception.class)
    public void notMap() throws Exception {
        BundleFilterMapPut filter = Configs.decodeObject(BundleFilterMapPut.class, "key:mykey,field:MYFIELD,map:MAP");
        Bundle b = new ListBundle();
        b.setValue(b.getFormat().getField("MYFIELD"), ValueFactory.create("blah"));
        filter.filter(b);
    }
}
