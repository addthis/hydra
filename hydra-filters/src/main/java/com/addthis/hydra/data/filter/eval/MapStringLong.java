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

public class MapStringLong extends AbstractMap<String, Long> implements Map<String, Long> {

    private final ValueMap<Long> data;

    public MapStringLong(ValueMap<Long> map, boolean copy) {
        if (copy) {
            this.data = ValueFactory.createMap();
            for (ValueMapEntry<Long> entry : map) {
                this.data.put(entry.getKey(), entry.getValue());
            }
        } else {
            this.data = map;
        }
    }

    public static ValueMap<Long> create(Map<String, Long> input) {
        if (input instanceof MapStringLong) {
            return ((MapStringLong) input).getData();
        } else {
            ValueMap<Long> output = ValueFactory.createMap();
            for (Map.Entry<String, Long> entry : input.entrySet()) {
                output.put(entry.getKey(), ValueFactory.create(entry.getValue()));
            }
            return output;
        }
    }

    public ValueMap<Long> getData() {
        return data;
    }

    @Override
    public Long get(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.get(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asLong().getLong();
        }
    }

    @Override
    public Long put(String key, Long value) {
        ValueObject val = data.put(key, ValueFactory.create(value));
        if (val == null) {
            return null;
        } else {
            return val.asLong().getLong();
        }
    }

    @Override
    public Long remove(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.remove(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asLong().getLong();
        }
    }

    private static class ViewIterator implements Iterator<Entry<String, Long>> {

        private final Iterator<Entry<String, ValueObject<Long>>> iterator;

        private ViewIterator(Iterator<Entry<String, ValueObject<Long>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<String, Long> next() {
            Entry<String, ValueObject<Long>> input = iterator.next();
            return new SimpleEntry<>(input.getKey(),
                    input.getValue().asLong().getLong());
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }

    private static class View extends AbstractSet<Entry<String, Long>> implements Set<Entry<String, Long>> {

        private final Set<Entry<String, ValueObject<Long>>> set;

        private View(Set<Entry<String, ValueObject<Long>>> set) {
            this.set = set;
        }

        @Override
        @Nonnull
        public Iterator<Entry<String, Long>> iterator() {
            return new ViewIterator(set.iterator());
        }

        @Override
        public int size() {
            return set.size();
        }
    }

    @Override
    @Nonnull
    public Set<Entry<String, Long>> entrySet() {
        return new View(data.entrySet());
    }
}
