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
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestValueFilterReverse {

    private ValueObject filterEach(ValueObject value) {
        return new ValueFilterReverse().filter(value);
    }

    private ValueObject filter(ValueObject value) {
        ValueFilterReverse f = new ValueFilterReverse();
        f.setOnce(true);
        return f.filter(value);
    }

    private ValueArray create(ValueObject value[]) {
        ValueArray a = ValueFactory.createArray(value.length);
        for (ValueObject v : value) {
            a.append(v);
        }
        return a;
    }


    @Test
    public void nullPassThrough() {
        assertEquals(null, filter(null));
        assertEquals(null, filterEach(null));
    }

    @Test
    public void charPassThrough() {
        ValueString obj = ValueFactory.create("z");
        assertEquals(obj, filter(obj));
        assertEquals(obj, filterEach(obj));
    }

    @Test
    public void singleElementPassThrough() {
        ValueArray expt = create(new ValueObject[]{ValueFactory.create("foo")});
        ValueArray arr = create(new ValueObject[]{ValueFactory.create("foo")});
        assertEquals(expt, filter(arr));
    }


    @Test
    public void stringRev() {
        ValueString obj = ValueFactory.create("abc");
        assertEquals("cba", ValueUtil.asNativeString(filter(obj)));
    }

    @Test
    public void arrayRev() {
        ValueArray arr = create(new ValueObject[]{ValueFactory.create("a"), ValueFactory.create("b"), ValueFactory.create("c")});
        ValueArray expt = create(new ValueObject[]{ValueFactory.create("c"), ValueFactory.create("b"), ValueFactory.create("a")});
        ValueObject rev = filter(arr);
        //System.out.println(expt);
        //System.out.println(rev);
        assertTrue(ValueUtil.isEqual(expt, rev));
    }

    @Test
    public void arrayRevEach() {
        ValueArray arr = create(new ValueObject[]{ValueFactory.create("foo"), ValueFactory.create("bar"), ValueFactory.create("bax")});
        ValueArray expt = create(new ValueObject[]{ValueFactory.create("oof"), ValueFactory.create("rab"), ValueFactory.create("xab")});
        ValueObject rev = filterEach(arr);
        System.out.println(expt);
        System.out.println(rev);
        assertTrue(ValueUtil.isEqual(expt, rev));
    }

}
