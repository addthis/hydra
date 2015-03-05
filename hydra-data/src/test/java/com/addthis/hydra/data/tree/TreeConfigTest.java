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
package com.addthis.hydra.data.tree;

import com.addthis.codec.config.Configs;
import com.addthis.codec.jackson.Jackson;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TreeConfigTest {
    private static final Logger log = LoggerFactory.getLogger(TreeConfigTest.class);

    @Test public void serialization() throws Exception {
        TreeConfig config = Configs.newDefault(TreeConfig.class);
        String val = Jackson.defaultMapper().writeValueAsString(config);
        assertEquals(config, Jackson.defaultMapper().readValue(val, TreeConfig.class));
    }

    @Test public void cacheWeight() throws Exception {
        TreeConfig config = Configs.decodeObject(TreeConfig.class, "unevictable = true");
        assertEquals(0.0d, config.cacheWeight(), 0.001);
        config = Configs.decodeObject(TreeConfig.class, "unevictable = false, cacheWeight = 5");
        assertNotEquals(0.0d, config.cacheWeight(), 0.001);
    }
}