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
package com.addthis.hydra.task.map;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.bundle.core.Bundles.decode;
import static com.addthis.bundle.util.CachingField.newAutoField;
import static com.addthis.bundle.value.ValueFactory.decodeValue;
import static com.addthis.codec.config.Configs.decodeObject;
import static org.junit.Assert.assertEquals;

public class FieldFilterTest {
    private static final Logger log = LoggerFactory.getLogger(FieldFilterTest.class);

    @Test
    public void mapField() throws Exception {
        FieldFilter fieldFilter = decodeObject(FieldFilter.class, "from: A, default: DEF");
        Bundle fromBundle = decode("C: C_VAL");
        Bundle toBundle = decode("D: D_VAL");
        fieldFilter.mapField(fromBundle, toBundle);
        log.debug("toBundle: {}", toBundle);
        AutoField field = newAutoField("A");
        assertEquals(decodeValue("DEF"), field.getValue(toBundle));
    }
}