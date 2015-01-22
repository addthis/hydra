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

public class ListBytes extends AbstractList<byte[]> implements List<byte[]> {

    private final ValueArray data;

    public ListBytes(ValueArray array, boolean copy) {
        if (copy) {
            int size = array.size();
            this.data = ValueFactory.createArray(size);
            for (int i = 0; i < size; i++) {
                this.data.add(array.get(i));
            }
        } else {
            this.data = array;
        }
    }

    public static ValueArray create(List<byte[]> input) {
        if (input == null) {
            return null;
        }
        if (input instanceof ListBytes) {
            return ((ListBytes) input).getData();
        } else {
            ValueArray output = ValueFactory.createArray(input.size());
            for (byte[] element : input) {
                output.add(ValueFactory.create(element));
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
    public boolean add(byte[] s) {
        data.add(ValueFactory.create(s));
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
    public byte[] get(int index) {
        ValueObject val = data.get(index);
        if (val == null) return null;
        return val.asBytes().asNative();
    }

    @Override
    public byte[] set(int index, byte[] element) {
        ValueObject prev = data.set(index, ValueFactory.create(element));
        if (prev == null) return null;
        return prev.asBytes().asNative();
    }

    @Override
    public void add(int index, byte[] element) {
        data.add(index, ValueFactory.create(element));
    }

    @Override
    public byte[] remove(int index) {
        ValueObject prev = data.remove(index);
        if (prev == null) return null;
        return prev.asBytes().asNative();
    }
}


