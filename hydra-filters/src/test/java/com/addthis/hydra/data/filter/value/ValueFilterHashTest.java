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

public class ValueFilterHashTest {

    @Test
    public void testFilterValue_sha1() throws Exception {
        ValueFilterHash valueFilterHash = new ValueFilterHash();
        valueFilterHash.setType(4);
        ValueObject vo = valueFilterHash.filter(ValueFactory.create("128.0.0.1"));
        assertEquals("9c678c09e0163cb9f0bdaf0363047ac5b549704e", vo.toString());
    }
}
