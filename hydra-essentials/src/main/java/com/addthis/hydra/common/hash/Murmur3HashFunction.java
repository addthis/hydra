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

import com.google.common.hash.Hashing;

public final class Murmur3HashFunction implements StringAndByteHashFunction {
    private static final Murmur3HashFunction INSTANCE = new Murmur3HashFunction();

    public static Murmur3HashFunction getInstance() {
        return INSTANCE;
    }

    public static int getHash(String input) {
        return Hashing.murmur3_32().hashUnencodedChars(input).asInt();
    }

    public static int getHash(byte[] input) {
        return Hashing.murmur3_32().hashBytes(input).asInt();
    }

    @Override public int hash(String input) {
        return Hashing.murmur3_32().hashUnencodedChars(input).asInt();
    }

    @Override public int hash(byte[] input) {
        return Hashing.murmur3_32().hashBytes(input).asInt();
    }
}
