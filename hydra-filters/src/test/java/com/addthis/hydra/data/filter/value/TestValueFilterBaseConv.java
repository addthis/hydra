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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterBaseConv {

    private ValueObject baseConvFilterDecode(String value, String baseType) {
        ValueFilterBaseConv vfbc = new ValueFilterBaseConv(true, baseType);
        return vfbc.filter(ValueFactory.create(value));
    }

    @Test
    public void fallThrough() {
        assertEquals(ValueFactory.create("100"), baseConvFilterDecode("100", "foo"));
    }

    @Test
    public void theo36() {
        assertEquals(ValueFactory.create("1296"), baseConvFilterDecode("100", "theo36"));
    }

    @Test
    public void theo64() {
        assertEquals(ValueFactory.create("4096"), baseConvFilterDecode("100", "theo64"));
    }


}
