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

    @Test
    public void emitFilter() throws IOException {
        map.put("hello", ValueFactory.create("is it me you're looking for?"));
        map.put("you say", ValueFactory.create("good-bye"));
        map.put("I say", ValueFactory.create("hello"));

        bundle.setValue(bundle.getFormat().getField("foo"), map);
        bundle.setValue(bundle.getFormat().getField("bar"), ValueFactory.create("lyrics"));

        builder = Configs.decodeObject(StreamMapSplitBuilder.class, "field: foo, keyField: key, valueField: value, emitFilter: {from: value, to: baz}");
        builder.process(bundle, emitter);

        ArgumentCaptor<Bundle> bundleCapture = ArgumentCaptor.forClass(Bundle.class);
        verify(emitter, times(3)).emit(bundleCapture.capture());
        List<Bundle> newBundles = bundleCapture.getAllValues();

        for (Bundle newBundle: newBundles) {
            assertEquals("lyrics", newBundle.getValue(newBundle.getFormat().getField("bar")).asString().asNative());
            String baz = newBundle.getValue(newBundle.getFormat().getField("baz")).asString().asNative();
            String key = newBundle.getValue(newBundle.getFormat().getField("key")).asString().asNative();
            String value = newBundle.getValue(newBundle.getFormat().getField("value")).asString().asNative();
            assertEquals("Filter to copy field didn't work", value, baz);
            assertTrue(key.equals("hello") || key.equals("you say") || key.equals("I say"));
            switch (key) {
                case "hello":
                    assertEquals("is it me you're looking for?", value);
                    break;
                case "you say":
                    assertEquals("good-bye", value);
                    break;
                case "I say":
                    assertEquals("hello", value);
                    break;
                default:
                    fail("An unexpected bundle was emitted");
                    break;
            }
        }
    }

    @Test
    public void emitFilterEmitOriginal() throws IOException {

        map.put("hello", ValueFactory.create("is it me you're looking for?"));
        map.put("you say", ValueFactory.create("good-bye"));
        map.put("I say", ValueFactory.create("hello"));

        bundle.setValue(bundle.getFormat().getField("foo"), map);
        bundle.setValue(bundle.getFormat().getField("bar"), ValueFactory.create("lyrics"));

        builder = Configs.decodeObject(StreamMapSplitBuilder.class, "field: foo, keyField: key, valueField: value, emitOriginal: true, emitFilter: {from.const: 42, to: baz}");
        builder.process(bundle, emitter);

        ArgumentCaptor<Bundle> bundleCapture = ArgumentCaptor.forClass(Bundle.class);
        verify(emitter, times(4)).emit(bundleCapture.capture());
        List<Bundle> newBundles = bundleCapture.getAllValues();

        int nullPreKey = 0;
        for (Bundle newBundle: newBundles) {
            assertEquals("lyrics", newBundle.getValue(newBundle.getFormat().getField("bar")).asString().asNative());
            long baz = newBundle.getValue(newBundle.getFormat().getField("baz")).asLong().asNative();
            final ValueObject preKey = newBundle.getValue(newBundle.getFormat().getField("key"));
            if (preKey == null) {
                nullPreKey++;
                continue;
            }
            String key = preKey.asString().asNative();
            String value = newBundle.getValue(newBundle.getFormat().getField("value")).asString().asNative();
            assertEquals("Constant filter value set didn't work", 42L, baz);
            assertTrue(key.equals("hello") || key.equals("you say") || key.equals("I say"));
            switch (key) {
                case "hello":
                    assertEquals("is it me you're looking for?", value);
                    break;
                case "you say":
                    assertEquals("good-bye", value);
                    break;
                case "I say":
                    assertEquals("hello", value);
                    break;
                default:
                    fail("An unexpected bundle was emitted");
                    break;
            }
        }
        assertEquals(1, nullPreKey);
    }
}
