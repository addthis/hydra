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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleBindingException;
import com.addthis.bundle.core.BundleException;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

/** */
public class TestBundleFilter {

    /* creates a map from an even number of String tokens in the pattern
     * key, value, key, value
     */
    public MapBundle createBundle(String[] pairs) {
        MapBundle bundle = new MapBundle();
        for (int i = 0; i < pairs.length - 1; i += 2) {
            bundle.put(pairs[i], pairs[i + 1]);
        }
        return bundle;
    }

    /* creates a Map that implements Bundle for easy testing */
    public MapBundle createBundle(Map<String, String> map) {
        return new MapBundle(map);
    }

    /* make JUnit happy */
    @Test
    public void doesNothing() {
    }

    /**
     * Helper method to get contents of a file as a String
     *
     * @param fileLocation
     * @return String
     */
    static public String getContents(String fileLocation) {
        StringBuilder contents = new StringBuilder();
        InputStream inputStream = TestBundleFilter.class.getClassLoader()
                .getResourceAsStream(fileLocation);
        try {
            //use buffering, reading one line at a time
            //FileReader always assumes default encoding is OK!
            BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
            try {
                String line = null; //not declared within while loop
                while ((line = input.readLine()) != null) {
                    contents.append(line);
                    contents.append(System.getProperty("line.separator"));
                }
            } finally {
                input.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return contents.toString();
    }

    protected class MapBundle extends HashMap<String, String> implements Bundle {

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

        /** */
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
        }

        /** */
        class MapBundleField implements BundleField {

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
    }
}
