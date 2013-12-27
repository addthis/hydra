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

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestValueFilterTrim {

    @Test
    public void testTrim() {
        ValueFilterTrim vft = new ValueFilterTrim();
        assertEquals("hi", vft.filter(" hi  \t"));
        // apparently the contract is to just pass through nulls
        assertEquals(null, vft.filter((String) null));
    }
}
