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

public class MapStringDouble extends AbstractMap<String, Double> implements Map<String, Double> {

    private final ValueMap<Double> data;

    public MapStringDouble(ValueMap<Double> map, boolean copy) {
        if (copy) {
            this.data = ValueFactory.createMap();
            for (ValueMapEntry<Double> entry : map) {
                this.data.put(entry.getKey(), entry.getValue());
            }
        } else {
            this.data = map;
        }
    }

    public static ValueMap create(Map<String, Double> input) {
        if (input instanceof MapStringDouble) {
            return ((MapStringDouble) input).getData();
        } else {
            ValueMap output = ValueFactory.createMap();
            for (Map.Entry<String, Double> entry : input.entrySet()) {
                output.put(entry.getKey(), ValueFactory.create(entry.getValue()));
            }
            return output;
        }
    }

    public ValueMap getData() {
        return data;
    }

    @Override
    public Double get(Object key) {
        String stringKey = (String) key;
        ValueObject val = data.get(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asDouble().getDouble();
        }
    }

    @Override
    public Double put(String key, Double value) {
        ValueObject val = data.put(key, ValueFactory.create(value));
        if (val == null) {
            return null;
        } else {
            return val.asDouble().getDouble();
        }
    }

    @Override
    public Double remove(Object key) {
        String stringKey = (String) key;
        ValueObject<Double> val = data.remove(stringKey);
        if (val == null) {
            return null;
        } else {
            return val.asDouble().getDouble();
        }
    }

    private static class ViewIterator implements Iterator<Entry<String, Double>> {

        private final Iterator<Entry<String, ValueObject<Double>>> iterator;

        private ViewIterator(Iterator<Entry<String, ValueObject<Double>>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Entry<String, Double> next() {
            Entry<String, ValueObject<Double>> input = iterator.next();
            return new SimpleEntry<>(input.getKey(),
                    input.getValue().asDouble().getDouble());
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }

    private static class View extends AbstractSet<Entry<String, Double>> implements Set<Entry<String, Double>> {

        private final Set<Entry<String, ValueObject<Double>>> set;

        private View(Set<Entry<String, ValueObject<Double>>> set) {
            this.set = set;
        }

        @Override
        @Nonnull
        public Iterator<Entry<String, Double>> iterator() {
            return new ViewIterator(set.iterator());
        }

        @Override
        public int size() {
            return set.size();
        }
    }

    @Override
    @Nonnull
    public Set<Entry<String, Double>> entrySet() {
        return new View(data.entrySet());
    }
}
