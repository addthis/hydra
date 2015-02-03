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

import com.addthis.codec.config.Configs;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestValueFilterGlob {

    @Test
    public void deser() throws IOException {
        ValueFilterGlob filter = (ValueFilterGlob) Configs.decodeObject(AbstractValueFilter.class, "glob = \"tr?an{g,z}l*\"");
    }

    @Test
    public void basics() throws IOException {
        ValueFilterGlob filter = Configs.decodeObject(ValueFilterGlob.class, "pattern = \"tr?an{g,z}l*\"");
        assertNotNull(filter.filter("triangles"));
        assertNotNull(filter.filter("tryanzl"));
        assertNull(filter.filter("rectangles"));
        assertNull(filter.filter((String) null));
    }
}
