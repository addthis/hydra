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

import java.util.Optional;
import java.util.Set;

import com.addthis.basis.kv.KVPairs;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class KVUtils {

    /** String values that are mapped to boolean {@code true}. */
    private static final Set<String> TRUE_STR_VALUES = Sets.newHashSet("true", "1", "on", "yes", "why not?");

    private static String findFirstNonNullValue(KVPairs kv, String[] keys) {
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

    private static String findFirstNonEmptyValue(KVPairs kv, String[] keys) {
        for (String key : keys) {
            if (kv.hasKey(key)) {
                String v = kv.getValue(key);
                if (!Strings.isNullOrEmpty(v)) {
                    return v;
                }
            }
        }
        return null;
    }

    /** Returns the first non-null value of the given sequence of keys. */
    public static String getValue(KVPairs kv, String defaultValue, String... keys) {
        String v = findFirstNonNullValue(kv, keys);
        return v == null ? defaultValue : v;
    }

    /**
     * Returns the first non-null value of the given sequence of keys as Long.
     * 
     * If all given keys have {@code null} value, the default is returned. Note that this method
     * stops scanning the keys upon finding the first non-null value, even if it's not a valid 
     * number (in which case, the default is returned, even a subsequent key may be a valid value).
     */
    public static Long getLongValue(KVPairs kv, Long defaultValue, String... keys) {
        String v = findFirstNonNullValue(kv, keys);
        try {
            return v == null ? defaultValue : Long.parseLong(v);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Returns the first non-empty value of the given sequence of keys as boolean.
     * 
     * If all given keys are {@code null} or empty, the default value is returned. This method
     * converts "true" and "1" to boolean {@code true}, and all other values to {@code false}.
     */
    public static boolean getBooleanValue(KVPairs kv, boolean defaultValue, String... keys) {
        String v = findFirstNonEmptyValue(kv, keys);
        return v == null ? defaultValue : TRUE_STR_VALUES.contains(v.toLowerCase());
    }
    
    /** 
     * Returns the non-null value of a given key wrapped in an {@link java.util.Optional}.
     * 
     * @return an empty Optional if the key doesn't exist or the value is {@code null}.
     */
    public static Optional<String> getValueOpt(KVPairs kv, String key) {
        return kv.hasKey(key) ? Optional.ofNullable(kv.getValue(key)) : Optional.empty();
    }
}
