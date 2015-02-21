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

import java.io.IOException;

import java.util.Collections;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static com.addthis.codec.config.Configs.decodeObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestValueFilterIndex {

    private ValueObject tokenFilter(ValueObject array, int index, boolean toNull) throws IOException {
        ValueFilter valueFilter = decodeObject(ValueFilterIndex.class, "index = " + index + ", toNull = " + toNull);
        return valueFilter.filter(array);
    }

    @Test public void contextualIndex() throws IOException {
        ValueFilter valueFilter = decodeObject(ValueFilterIndex.class, "index.field = idx");
        Bundle bundle = Bundles.decode("idx = 2");
        ValueArray arr = ValueFactory.decodeValue("[a, b, c, d]").asArray();
        assertEquals(valueFilter.filter(arr, bundle), arr.get(2));
    }

    @Test
    public void emptyPassThrough() throws IOException {
        ValueArray arr = ValueFactory.createArray(1);
        assertNull(tokenFilter(arr, 0, true));
    }

    private ValueArray create(ValueObject[] value) {
        ValueArray a = ValueFactory.createArray(value.length);
        Collections.addAll(a, value);
        return a;
    }

    @Test
    public void tokenize() throws IOException {
        ValueObject foo = ValueFactory.create("foo");
        ValueObject bar = ValueFactory.create("bar");
        ValueObject bax = ValueFactory.create("bax");
        assertEquals(foo, tokenFilter(create(new ValueObject[]{foo, bar, bax}), 0, false));
        assertEquals(bar, tokenFilter(create(new ValueObject[]{foo, bar, bax}), 1, false));
        assertEquals(bax, tokenFilter(create(new ValueObject[]{foo, bar, bax}), 2, false));
    }
}
