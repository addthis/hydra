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

import java.util.List;

import com.addthis.hydra.common.plugins.PluginReader;

import com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PluggableHashFunction {

    private static final Logger log = LoggerFactory.getLogger(PluggableHashFunction.class);

    private static final StringAndByteHashFunction hasher;

    static {
        StringAndByteHashFunction hashFunction;
        hashFunction = new DefaultHashFunction();
        try {
            List<String[]> filters = PluginReader.readProperties("pluggable-hash-function.property");
            if (filters.size() > 0 && filters.get(0).length > 0) {
                Class clazz = Class.forName(filters.get(0)[0]);
                hashFunction = (StringAndByteHashFunction) clazz.newInstance();
            }
        } catch (ClassNotFoundException|InstantiationException|IllegalAccessException ex) {
            log.warn(ex.toString());
        }
        hasher = hashFunction;
    }

    public interface StringAndByteHashFunction {

        public int hash(String input);

        public int hash(byte[] input);
    }

    private static final class DefaultHashFunction implements StringAndByteHashFunction {

        public int hash(String input) {
            return Hashing.murmur3_32().hashUnencodedChars(input).asInt();
        }

        public int hash(byte[] input) {
            return Hashing.murmur3_32().hashBytes(input).asInt();
        }
    }

    public static int hash(String input) {
        return hasher.hash(input);
    }

    public static int hash(byte[] input) {
        return hasher.hash(input);
    }

}
