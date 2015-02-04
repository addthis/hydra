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
package com.addthis.hydra.store.kv;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface KeyCoder<K, V> {

    K negInfinity();

    /**
     * Unoptimized key encoding. Uses only the state of the key
     * to generate byte array. The sorted order of two keys should
     * be equal to the natural ordering of their corresponding byte arrays.
     *
     * @param key
     * @return
     */
    byte[] keyEncode(@Nullable K key, @Nonnull TreeEncodeType encodeType);

    /**
     * Optimized key encoding. Can use the base key to generate
     * a smaller byte array.
     *
     * @param key
     * @param baseKey
     * @param encodeType
     * @return
     */
    byte[] keyEncode(@Nullable K key, @Nonnull K baseKey, @Nonnull PageEncodeType encodeType);

    byte[] valueEncode(V value, PageEncodeType encodeType);

    /**
     * Unoptimized key encoding. Uses only the state of the
     * byte array to generate the key. The sorted order of
     * two keys should be equal to the natural ordering of
     * their corresponding byte arrays.
     *
     * @param key
     * @return
     */
    K keyDecode(byte[] key, @Nonnull TreeEncodeType encodeType);

    /**
     * Optimized key decoding. Can use the byte array
     * and the base key to generate the key.
     *
     * @param key
     * @param baseKey
     * @param encodeType
     * @return
     */
    K keyDecode(@Nullable byte[] key, @Nonnull K baseKey, @Nonnull PageEncodeType encodeType);

    V valueDecode(byte[] value, @Nonnull PageEncodeType encodeType);

    /**
     * throws a NullPointerException if the input is null.
     *
     * @param value a non-null value
     * @return true if-and-only-if the input encodes the null value.
     */
    boolean nullRawValueInternal(byte[] value);
}
