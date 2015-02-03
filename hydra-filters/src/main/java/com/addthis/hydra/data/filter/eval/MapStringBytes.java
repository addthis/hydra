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
package com.addthis.hydra.data.filter.eval;

import javax.annotation.Nonnull;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;

public class MapStringBytes extends AbstractMap<String, byte[]> implements Map<String, byte[]> {

    private final ValueMap data;

    public MapStringBytes(ValueMap map, boolean copy) {
        if (copy) {
            this.data = ValueFactory.createMap();
            for (ValueMapEntry entry : map) {
                this.data.put(entry.getKey(), entry.getValue());
            }
        } else {
            this.data = map;
        }
    }

    public static ValueMap create(Map<String, byte[]> input) {
        if (input == null) {
            return null;
        }
        if (input instanceof MapStringBytes) {
            return ((MapStringBytes) input).getData();
        } else {
            ValueMap output = ValueFactory.createMap();
            for (Map.Entry<String, byte[]> entry : input.entrySet()) {
                output.put(entry.getKey(), ValueFactory.create(entry.getValue()));
            }
            return output;
        }
    }

    public ValueMap getData() {
        return data;
    }

    @Override
    public byte[] get(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.get(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asBytes().asNative();
        }
    }

    @Override
    public byte[] put(String key, byte[] value) {
        ValueObject val = data.put(key, ValueFactory.create(value));
        if (val == null) {
            return null;
        } else {
            return val.asBytes().asNative();
        }
    }

    @Override
    public byte[] remove(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.remove(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asBytes().asNative();
        }
    }

    private static class ViewIterator implements Iterator<Entry<String, byte[]>> {

        private final Iterator<Entry<String, ValueObject>> iterator;

        private ViewIterator(Iterator<Entry<String, ValueObject>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<String, byte[]> next() {
            Entry<String, ValueObject> input = iterator.next();
            return new SimpleEntry<>(input.getKey(),
                    input.getValue().asBytes().asNative());
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }

    private static class View extends AbstractSet<Entry<String, byte[]>> implements Set<Entry<String, byte[]>> {

        private final Set<Entry<String, ValueObject>> set;

        private View(Set<Entry<String, ValueObject>> set) {
            this.set = set;
        }

        @Override
        @Nonnull
        public Iterator<Entry<String, byte[]>> iterator() {
            return new ViewIterator(set.iterator());
        }

        @Override
        public int size() {
            return set.size();
        }
    }

    @Override
    @Nonnull
    public Set<Entry<String, byte[]>> entrySet() {
        return new View(data.entrySet());
    }
}
