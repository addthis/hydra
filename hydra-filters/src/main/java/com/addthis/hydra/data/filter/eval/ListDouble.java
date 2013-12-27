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

import java.util.AbstractList;
import java.util.List;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

public class ListDouble extends AbstractList<Double> implements List<Double> {

    private final ValueArray data;

    public ListDouble(ValueArray array, boolean copy) {
        if (copy) {
            int size = array.size();
            this.data = ValueFactory.createArray(size);
            for (int i = 0; i < size; i++) {
                this.data.append(data.get(i));
            }
        } else {
            this.data = array;
        }
    }

    public static ValueArray create(List<Double> input) {
        if (input instanceof ListDouble) {
            return ((ListDouble) input).getData();
        } else {
            ValueArray output = ValueFactory.createArray(input.size());
            for (Double element : input) {
                output.append(ValueFactory.create(element));
            }
            return output;
        }
    }

    public ValueArray getData() {
        return data;
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean add(Double s) {
        data.append(ValueFactory.create(s));
        return true;
    }

    @Override
    public boolean remove(Object o) {
        for (int i = 0; i < data.size(); i++) {
            ValueObject next = data.get(i);
            if ((next != null && next.equals(o)) || (next == null && o == null)) {
                data.remove(i);
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public Double get(int index) {
        ValueObject val = data.get(index);
        if (val == null) return null;
        return val.asDouble().getDouble();
    }

    @Override
    public Double set(int index, Double element) {
        ValueObject prev = data.set(index, ValueFactory.create(element));
        if (prev == null) return null;
        return prev.asDouble().getDouble();
    }

    @Override
    public void add(int index, Double element) {
        data.insert(index, ValueFactory.create(element));
    }

    @Override
    public Double remove(int index) {
        ValueObject prev = data.remove(index);
        if (prev == null) return null;
        return prev.asDouble().getDouble();
    }


}
