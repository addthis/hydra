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
package com.addthis.hydra.task.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;

import java.io.IOException;

import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.config.Configs;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class StreamMapSplitBuilderTest {
    StreamEmitter emitter;
    Bundle bundle;
    ValueMap map;
    StreamMapSplitBuilder builder;
    
    @Before
    public void setup() throws IOException {
        emitter = mock(StreamEmitter.class);
        bundle = new ListBundle();
        map = ValueFactory.createMap();
        builder = Configs.decodeObject(StreamMapSplitBuilder.class, "field: foo, keyField: key, valueField: value");
    }
    
    @Test
    public void basicFunctionality() {
        map.put("hello", ValueFactory.create("boo"));
        map.put("hi", ValueFactory.create("hoo"));
        ValueMap map2 = ValueFactory.createMap();
        map2.put("deep", ValueFactory.create("deeper"));
        map.put("nest", map2);
        bundle.setValue(bundle.getFormat().getField("foo"), map);
        bundle.setValue(bundle.getFormat().getField("bar"), ValueFactory.create("biz"));
        builder.process(bundle, emitter);
        
        ArgumentCaptor<Bundle> bundleCapture = ArgumentCaptor.forClass(Bundle.class);
        verify(emitter, times(3)).emit(bundleCapture.capture());
        List<Bundle> newBundles = bundleCapture.getAllValues();
        
        for (Bundle newBundle : newBundles) {
            assertEquals("biz", newBundle.getValue(newBundle.getFormat().getField("bar")).asString().asNative());
            String key = newBundle.getValue(newBundle.getFormat().getField("key")).asString().asNative();
            assertTrue(key.equals("hello") || key.equals("hi") || key.equals("nest"));
            if (key.equals("hello")) {
                assertEquals("boo", newBundle.getValue(newBundle.getFormat().getField("value")).asString().asNative());
            } else if (key.equals("hi")) {
                assertEquals("hoo", newBundle.getValue(newBundle.getFormat().getField("value")).asString().asNative());
            } else if (key.equals("nest")) {
                assertEquals(ValueObject.TYPE.MAP, newBundle.getValue(newBundle.getFormat().getField("value")).getObjectType());
            } else {
                fail("An unexpected bundle was emitted");
            }
        }
    }
    
    @Test
    public void noErrorsShouldBeThrown() {
        // there is no map
        builder.process(bundle, emitter);
        // map not a map
        bundle.setValue(bundle.getFormat().getField("foo"), ValueFactory.create("notamap"));
        builder.process(bundle, emitter);
        verify(emitter, never()).emit(any());
    }
    
    @Test
    public void emitOriginal() throws IOException {
        map.put("hello", ValueFactory.create("boo"));
        map.put("hi", ValueFactory.create("hoo"));
        bundle.setValue(bundle.getFormat().getField("foo"), map);
        bundle.setValue(bundle.getFormat().getField("bar"), ValueFactory.create("biz"));
        builder = Configs.decodeObject(StreamMapSplitBuilder.class, "field: foo, keyField: key, valueField: value, emitOriginal: true");
        builder.process(bundle, emitter);
        ArgumentCaptor<Bundle> bundleCapture = ArgumentCaptor.forClass(Bundle.class);
        verify(emitter, times(3)).emit(bundleCapture.capture());
        List<Bundle> newBundles = bundleCapture.getAllValues();
        int originalSeen = 0;
        for (Bundle b : newBundles) {
            if (b.getValue(b.getFormat().getField("key")) == null) {
                originalSeen++;
                assertEquals("biz", b.getValue(b.getFormat().getField("bar")).asString().asNative());
                assertNull(b.getValue(b.getFormat().getField("foo")));
            }
        }
        assertEquals(1, originalSeen);
    }
    
    @Test
    public void emitOriginalNoMapField() throws IOException {
        bundle.setValue(bundle.getFormat().getField("bar"), ValueFactory.create("biz"));
        builder = Configs.decodeObject(StreamMapSplitBuilder.class, "field: foo, keyField: key, valueField: value, emitOriginal: false");
        builder.process(bundle, emitter);
        verify(emitter, never()).emit(any());;
    }
}
