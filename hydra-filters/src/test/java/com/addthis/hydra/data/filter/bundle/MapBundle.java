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

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleBindingException;
import com.addthis.bundle.core.BundleException;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonCreator;

class MapBundle extends TreeMap<String, String> implements Bundle {

    MapBundle() {
        super();
    }

    MapBundle(Map<String, String> map) {
        super(map);
    }

    @Override
    public BundleFormat getFormat() {
        return new MapBundleFormat();
    }

    @Override
    public ValueObject getValue(BundleField field) throws BundleException {
        return ValueFactory.create(get(field.getName()));
    }

    @Override
    public void setValue(BundleField field, ValueObject value) throws BundleBindingException {
        if (value != null && value instanceof ValueArray) {
            StringBuilder sb = new StringBuilder();
            int count = 0;
            for (ValueObject valueObject : (ValueArray) value) {
                if (count++ > 0) {
                    sb.append(",");
                }
                sb.append(valueObject.asString().toString());
            }
            put(field.getName(), sb.toString());
        } else if (value != null) {
            put(field.getName(), value.asString().toString());
        }
    }

    class MapBundleFormat implements BundleFormat {

        @Override
        public BundleField getField(String name) {
            return new MapBundleField(name);
        }

        @Override
        public boolean hasField(String name) {
            return MapBundle.this.containsKey(name);
        }

        @Override
        public Iterator<BundleField> iterator() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Object getVersion() {
            return null;
        }

        @Override
        public BundleField getField(int pos) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int getFieldCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override public Bundle createBundle() {
            return new MapBundle();
        }
    }

    static class MapBundleField implements BundleField {

        private String name;

        MapBundleField(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Integer getIndex() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MapBundleField that = (MapBundleField) o;

            if (!name.equals(that.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    class MapBundleFieldIterator implements Iterator<BundleField> {

        private final Iterator<String> fields;

        MapBundleFieldIterator() {
            fields = MapBundle.this.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return fields.hasNext();
        }

        @Override
        public BundleField next() {
            if (hasNext()) {
                return new MapBundleField(fields.next());
            } else {
                return null;
            }
        }

        @Override
        public void remove() {
            fields.remove();
        }
    }

    @Override
    public Iterator<BundleField> iterator() {
        return new MapBundleFieldIterator();
    }

    @Override
    public void removeValue(BundleField field) throws BundleException {
        // TODO Auto-generated method stub
    }

    @Override
    public int getCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Bundle createBundle() {
        // TODO Auto-generated method stub
        return null;
    }

    /* Creates a map from an even number of String tokens in the pattern key, value, key, value. */
    public static MapBundle createBundle(String[] pairs) {
        MapBundle bundle = new MapBundle();
        for (int i = 0; i < (pairs.length - 1); i += 2) {
            bundle.put(pairs[i], pairs[i + 1]);
        }
        return bundle;
    }

    /* Creates a Map that implements Bundle for easy testing. */
    @JsonCreator public static MapBundle createBundle(Map<String, String> map) {
        return new MapBundle(map);
    }
}
