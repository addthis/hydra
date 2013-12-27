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
package com.addthis.hydra.store.util;

import org.junit.Assert;
import org.junit.Test;

public class TestSeenFilterGhetto {

    @Test
    public void test() {
        int bits = 40000;
        SeenFilterBasic<?> sfg = new SeenFilterBasic(bits, 4, 2);
        for (int i = 0; i < bits; i++) {
            Assert.assertFalse(sfg.getBit(i));
            sfg.setBit(i);
            Assert.assertTrue(sfg.getBit(i));
        }
        System.out.println("tested " + bits + " bits");
    }
}
