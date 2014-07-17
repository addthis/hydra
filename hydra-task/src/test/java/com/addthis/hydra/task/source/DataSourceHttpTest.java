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
package com.addthis.hydra.task.source;

import com.addthis.codec.config.CodecConfig;
import com.addthis.codec.config.ConfigCodable;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;

import org.junit.Test;

public class DataSourceHttpTest {

    @Test
    public void rando() throws Exception {
        CodecConfig.getDefault().decodeObject(ConfigHolder.class, "{_heldValue: [1, 2, 3]}");
    }

    public static class ConfigHolder implements ConfigCodable {
        private Config heldConfig;

        @Override public ConfigObject toConfigObject() {
            throw new UnsupportedOperationException("only meant for decoding");
        }

        @Override public void fromConfigObject(ConfigObject config, ConfigObject defaults) {
            heldConfig = config.toConfig();
        }
    }
}