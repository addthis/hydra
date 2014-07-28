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

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

/**
 * Method signatures and name are similar to but not compatible with other classes in the package.
 * Colliding method names are deprecated for eventual resolution.
 */
public final class MD5HashFunction {
    private MD5HashFunction() {}

    public static String hashAsString(byte[] bytes) {
        HashCode hc = Hashing.md5().hashBytes(bytes);
        return hc.toString();
    }

    /** @deprecated Use {@link #hashAsString(byte[])}  */
    @Deprecated
    public static String hash(byte[] bytes) {
        return hashAsString(bytes);
    }

    public static String hashAsString(String key) {
        HashCode hc = Hashing.md5().hashUnencodedChars(key);
        return hc.toString();
    }

    /** @deprecated Use {@link #hashAsString(String)}. */
    @Deprecated
    public static String hash(String key) {
        return hashAsString(key);
    }
}
