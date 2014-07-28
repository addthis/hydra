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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">sorts an array</span>.
 * <p/>
 * <p>Items in the array are sorted according to the natural ordering of their underlying type.
 * Only arrays with elements of TYPE STRING, INT, or FLOAT are supported. All elements of the array must be from the same type.
 * </p>
 */
public class ValueFilterSort extends ValueFilter {

    private static final Comparator<ValueObject> valueObjectComparator = new Comparator<ValueObject>() {
        @Override
        public int compare(ValueObject o1, ValueObject o2) {
            if (o1.getObjectType() != o2.getObjectType()) {
                throw new RuntimeException("Sort error: different object types: " + o1.getObjectType() + "," + o2.getObjectType());
            }
            switch (o1.getObjectType()) {
                case STRING:
                    return o1.asString().asNative().compareTo(o2.asString().asNative());
                case INT:
                    return Long.compare(o1.asLong().getLong(), o2.asLong().getLong()); // DefaultLong.TYPE = INT, go figure
                case FLOAT:
                    return Double.compare(o1.asDouble().getDouble(), o2.asDouble().getDouble()); // ... and DefaultDouble.TYPE = FLOAT
                default:
                    throw new RuntimeException("Sort error: unsupported object type " + o1.getObjectType());
            }
        }
    };

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return null;
        }
        ValueObject.TYPE type = value.getObjectType();
        switch (type) {
            case ARRAY:
                return sortArray(value.asArray());
            case INT:
                return value;
            case FLOAT:
                return value;
            case STRING:
                return value;
            default:
                throw new RuntimeException("Unsupported object type " + type);
        }
    }

    public static ValueArray sortArray(ValueArray array) {
        List<ValueObject> tmpObjs = new ArrayList<>();
        for (ValueObject obj : array) {
            tmpObjs.add(obj);
        }
        Collections.sort(tmpObjs, valueObjectComparator);
        ValueArray rv = ValueFactory.createArray(array.size());
        for (ValueObject obj : tmpObjs) {
            rv.add(obj);
        }
        return rv;
    }
}
