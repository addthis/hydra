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

import java.util.Arrays;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterSort {

    @Test
    public void sortTest() {
        ValueArray array = ValueFactory.createValueArray(Arrays.asList("aa", "bb", "ab", "ba"));
        ValueFilterSort sort = new ValueFilterSort();
        assertEquals("should sort array correctly", "[aa, ab, ba, bb]", sort.filterValue(array).toString());
    }
}
