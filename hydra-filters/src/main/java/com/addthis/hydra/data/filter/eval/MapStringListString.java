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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;

public class MapStringListString extends AbstractMap<String, List<String>> implements Map<String, List<String>> {

    private final ValueMap data;

    public MapStringListString(ValueMap map, boolean copy) {
        if (copy) {
            this.data = ValueFactory.createMap();
            for (ValueMapEntry entry : map) {
                ValueArray array = entry.getValue().asArray();
                int size = array.size();
                ValueArray copyArray = ValueFactory.createArray(size);
                for (int i = 0; i < size; i++) {
                    copyArray.add(array.get(i));
                }
                this.data.put(entry.getKey(), copyArray);
            }
        } else {
            this.data = map;
        }
    }

    public static ValueMap create(Map<String, List<String>> input) {
        if (input == null) {
            return null;
        }
        if (input instanceof MapStringListString) {
            return ((MapStringListString) input).getData();
        } else {
            ValueMap output = ValueFactory.createMap();
            for (Entry<String, List<String>> entry : input.entrySet()) {
                output.put(entry.getKey(), ListString.create(entry.getValue()));
            }
            return output;
        }
    }

    public ValueMap getData() {
        return data;
    }

    @Override
    public List<String> get(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.get(stringKey);
        if (val == null) {
            return null;
        } else {
            return new ListString(val.asArray(), false);
        }
    }

    @Override
    public List<String> put(String key, List<String> value) {
        ValueObject val = data.put(key, ListString.create(value));
        if (val == null) {
            return null;
        } else {
            return new ListString(val.asArray(), false);
        }
    }

    @Override
    public List<String> remove(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.remove(stringKey);
        if (val == null) {
            return null;
        } else {
            return new ListString(val.asArray(), false);
        }
    }

    private static class ViewIterator implements Iterator<Entry<String, List<String>>> {

        private final Iterator<Entry<String, ValueObject>> iterator;

        private ViewIterator(Iterator<Entry<String, ValueObject>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<String, List<String>> next() {
            Entry<String, ValueObject> input = iterator.next();
            return new SimpleEntry<>(input.getKey(),
                    new ListString(input.getValue().asArray(), false));
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }

    private static class View extends AbstractSet<Entry<String, List<String>>> implements Set<Entry<String, List<String>>> {

        private final Set<Entry<String, ValueObject>> set;

        private View(Set<Entry<String, ValueObject>> set) {
            this.set = set;
        }

        @Override
        @Nonnull
        public Iterator<Entry<String, List<String>>> iterator() {
            return new ViewIterator(set.iterator());
        }

        @Override
        public int size() {
            return set.size();
        }
    }

    @Override
    @Nonnull
    public Set<Entry<String, List<String>>> entrySet() {
        return new View(data.entrySet());
    }
}
