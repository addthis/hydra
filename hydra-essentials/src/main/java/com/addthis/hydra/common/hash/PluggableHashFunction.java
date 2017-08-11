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
package com.addthis.hydra.common.hash;

import com.addthis.codec.plugins.PluginMap;
import com.addthis.codec.plugins.PluginRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluggableHashFunction {

    private static final Logger log = LoggerFactory.getLogger(PluggableHashFunction.class);

    private static final StringAndByteHashFunction hasher;

    static {
        StringAndByteHashFunction hashFunction = Murmur3HashFunction.getInstance();
        try {
            PluginMap hashFunctions = PluginRegistry.defaultRegistry().asMap().get("hash function");
            Class<?> defaultHashFunctionClass = hashFunctions.defaultSugar();
            if (defaultHashFunctionClass != null) {
                hashFunction = (StringAndByteHashFunction) defaultHashFunctionClass.newInstance();
            } else {
                log.warn("No default hash function was configured. Using Murmur Hash 3, but " +
                         "even though this is the library default, this condition is not " +
                         "expected. Check your configuration!");
            }
        } catch (Exception ex) {
            log.warn("Unexpected error trying to load a pluggable hash function", ex);
        }
        hasher = hashFunction;
    }

    public static int hash(String input) {
        return hasher.hash(input);
    }

    public static int hash(byte[] input) {
        return hasher.hash(input);
    }

}
