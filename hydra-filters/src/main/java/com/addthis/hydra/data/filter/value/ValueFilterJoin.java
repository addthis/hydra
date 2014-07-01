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
package com.addthis.hydra.data.filter.value;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">joins an array or a map to a string</span>.
 * <p/>
 * <p>Items of the sequence are separated by the {@link #join join} field.
 * If the input is a map, then (key, value) pairs are separated by the {@link #keyJoin keyJoin} field.
 * For arrays only, the values can be sorted prior to being joined into a String.
 * Optionally, the filter can be used to filter elements of the array or values of the map.
 * The keyFilter can be used to filter out keys of the map. If the input is a map,
 * then the filter and the keyFilter operate independently of each other. When filters are applied
 * on a map, it is possible for elements in the output to have a key with no value or a value with no key.
 * </p>
 * <p>
 * <p>Example:</p>
 * <pre>
 *
 * </pre>
 *
 * @user-reference
 * @hydra-name join
 * @exclude-fields once
 */
public class ValueFilterJoin extends ValueFilter {

    /**
     * The deliminator between elements in the output string. Default is "," .
     */
    @FieldConfig(codable = true)
    private String join = ",";

    /**
     * If the input is a map, then the deliminator between keys and values. Default is "=" .
     */
    @FieldConfig(codable = true)
    private String keyJoin = "=";

    /**
     * If the input is an array or a map, then optional filter to apply onto values. Default is
     * null.
     */
    @FieldConfig(codable = true)
    private ValueFilter filter;

    /**
     * If the input is a map, then optional filter to apply onto keys. Default is null.
     */
    @FieldConfig(codable = true)
    private ValueFilter keyFilter;

    /**
     * If the input is a map or array, then sort the keys before joining. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean sort;

    public ValueFilterJoin setJoin(String join) {
        this.join = join;
        return this;
    }

    public ValueFilterJoin setKeyJoin(String keyJoin) {
        this.keyJoin = keyJoin;
        return this;
    }

    public ValueFilterJoin setFilter(ValueFilter filter) {
        this.filter = filter;
        return this;
    }

    public ValueFilterJoin setKeyFilter(ValueFilter keyFilter) {
        this.keyFilter = keyFilter;
        return this;
    }

    public ValueFilterJoin setSort(boolean sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        return filter != null ? filter.filter(value) : value;
    }

    private String filterKey(String value) {
        return keyFilter != null ?
               ValueUtil.asNativeString(keyFilter.filter(ValueFactory.create(value))) :
               value;
    }

    @Override
    public ValueObject filter(ValueObject value) {
        if (value == null) {
            return null;
        }
        if (value.getObjectType() == ValueObject.TYPE.ARRAY) {
            int count = 0;
            StringBuffer sb = new StringBuffer();
            ValueArray array = value.asArray();
            if (sort) {
                array = ValueFilterSort.sortArray(array);
            }
            for (ValueObject el : array) {
                if (count++ > 0) {
                    sb.append(join);
                }
                sb.append(ValueUtil.asNativeString(filterValue(el)));
            }
            return ValueFactory.create(sb.toString());
        } else if (value.getObjectType() == ValueObject.TYPE.MAP) {
            int count = 0;
            StringBuffer sb = new StringBuffer();
            ValueMap<?> map = value.asMap();
            if (sort) {
                // ValueMap is a wrapper around HashMap; it is unordered
                throw new RuntimeException("Unsupported operation: cannot sort map input");
            }
            for (ValueMapEntry<?> e : map) {
                if (count++ > 0) {
                    sb.append(join);
                }
                sb.append(filterKey(e.getKey()));
                sb.append(keyJoin);
                sb.append(ValueUtil.asNativeString(filterValue(e.getValue())));
            }
            return ValueFactory.create(sb.toString());
        }
        return value;
    }
}
