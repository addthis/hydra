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

public class MapStringString extends AbstractMap<String, String> implements Map<String, String> {

    private final ValueMap data;

    public MapStringString(ValueMap map, boolean copy) {
        if (copy) {
            this.data = ValueFactory.createMap();
            for (ValueMapEntry entry : map) {
                this.data.put(entry.getKey(), entry.getValue());
            }
        } else {
            this.data = map;
        }
    }

    public static ValueMap create(Map<String, String> input) {
        if (input instanceof MapStringString) {
            return ((MapStringString) input).getData();
        } else {
            ValueMap output = ValueFactory.createMap();
            for (Map.Entry<String, String> entry : input.entrySet()) {
                output.put(entry.getKey(), ValueFactory.create(entry.getValue()));
            }
            return output;
        }
    }

    public ValueMap getData() {
        return data;
    }

    @Override
    public String get(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.get(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asString().asNative();
        }
    }

    @Override
    public String put(String key, String value) {
        ValueObject val = data.put(key, ValueFactory.create(value));
        if (val == null) {
            return null;
        } else {
            return val.asString().asNative();
        }
    }

    @Override
    public String remove(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.remove(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asString().asNative();
        }
    }

    private static class ViewIterator implements Iterator<Entry<String, String>> {

        private final Iterator<Entry<String, ValueObject>> iterator;

        private ViewIterator(Iterator<Entry<String, ValueObject>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<String, String> next() {
            Entry<String, ValueObject> input = iterator.next();
            return new AbstractMap.SimpleEntry<>(input.getKey(),
                    input.getValue().asString().asNative());
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }

    private static class View extends AbstractSet<Entry<String, String>> implements Set<Entry<String, String>> {

        private final Set<Entry<String, ValueObject>> set;

        private View(Set<Entry<String, ValueObject>> set) {
            this.set = set;
        }

        @Override
        @Nonnull
        public Iterator<Entry<String, String>> iterator() {
            return new ViewIterator(set.iterator());
        }

        @Override
        public int size() {
            return set.size();
        }
    }

    @Override
    @Nonnull
    public Set<Entry<String, String>> entrySet() {
        return new View(data.entrySet());
    }
}
