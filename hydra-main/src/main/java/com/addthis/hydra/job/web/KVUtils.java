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
package com.addthis.hydra.job.web;

import java.util.Set;

import com.addthis.basis.kv.KVPairs;

import com.google.common.collect.Sets;

public class KVUtils {

    /** String values that are mapped to boolean {@code true} */
    private static final Set<String> TRUE_STR_VALUES = Sets.newHashSet("true", "1");

    public static String getValue(KVPairs kv, String[] keys) {
        for (String key : keys) {
            if (kv.hasKey(key)) {
                String v = kv.getValue(key);
                if (v != null) {
                    return v;
                }
            }
        }
        return null;
    }

    public static String getValue(KVPairs kv, String defaultValue, String... keys) {
        String v = getValue(kv, keys);
        return v == null ? defaultValue : v;
    }

    public static Long getLongValue(KVPairs kv, Long defaultValue, String... keys) {
        String v = getValue(kv, keys);
        try {
            return v == null ? defaultValue : Long.parseLong(v);
        } catch (Exception e) {
            return null;
        }
    }

    public static boolean getBooleanValue(KVPairs kv, boolean defaultValue, String... keys) {
        String v = getValue(kv, keys);
        return v == null ? defaultValue : TRUE_STR_VALUES.contains(v);
    }
}
