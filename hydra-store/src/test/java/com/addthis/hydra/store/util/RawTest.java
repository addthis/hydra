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

import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RawTest {

    @Test
    public void sortWithBytes() {
        sort(false);
    }

    @Test
    public void sortWithLongs() {
        sort(true);
    }

    //	@Test
    public void timeSortMethods() {
        for (int j = 0; j < 5; j++) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                sort(false);
            }
            System.out.println(j + "] byte sort in " + (System.currentTimeMillis() - time));
            time = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                sort(true);
            }
            System.out.println(j + "] long sort in " + (System.currentTimeMillis() - time));
        }
    }

    public void sort(boolean longCompare) {
        Raw.useLongCompare(longCompare);
        String s[] = new String[]{
                "acbd",
                "zyx",
                "abcd",
                "abc",
                "1234567890",
                "xyz",
                "abcde",
                "abcdefghijklmnpo",
                "abcdefghijklmnoo",
                "abcdefghijklmnopslkfjlsdfj",
                "abcdefghijklmnopslksfslkdfjkl",
                "abcde123ijklmnop",
                "acb",
        };
        Raw r[] = new Raw[s.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = Raw.get(s[i]);
        }
        Arrays.sort(s);
        Arrays.sort(r);
        for (int i = 0; i < r.length; i++) {
            assertEquals("lc=" + longCompare + " " + s[i] + " = " + r[i], s[i], r[i].toString());
        }
    }
}
