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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestValueFilterGlob {

    @Test
    public void testBasic() {
        String pattern = "tr?an{g,z}l*";
        ValueFilterGlob filter = new ValueFilterGlob();
        filter.setPattern(pattern);
        assertNotNull(filter.filter("triangles"));
        assertNotNull(filter.filter("tryanzl"));
        assertNull(filter.filter("rectangles"));
        String nullstring = null;
        assertNull(filter.filter(nullstring));
    }
}
