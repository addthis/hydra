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
import static org.junit.Assert.assertTrue;

public class TestValueFilterRegex {

    @Test
    public void akamaiGroup() {
        ValueFilterRegex vf = new ValueFilterRegex().setPattern("Log_([0-9]+)\\.");
        ValueObject res = vf.filter(ValueFactory.create("stream://san1.local:8614/split/logs/12345/2011/05/20/aLog_12345.esw3c_U.201105200000-0100-1.gz"));
        assertTrue(res != null);
        assertTrue(res.asArray().size() > 0);
        assertEquals("12345", res.asArray().get(0).asString().toString());
    }
}
