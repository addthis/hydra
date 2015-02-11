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

    /**
     * Returns the smallest possible key value.
     *
     * @return
     */
    K negInfinity();

    /**
     * Unoptimized key encoding. Uses only the state of the key
     * to generate byte array. The sorted order of two keys should
     * be equal to the natural ordering of their corresponding byte arrays.
     *
     * @param key           input to encode
     * @return key serialization to byte array
     */
    byte[] keyEncode(@Nullable K key);

    /**
     * Optimized key encoding. Can use the base key to generate
     * a smaller byte array.
     *
     * @param key           input to encode
     * @param baseKey       another key value that can be used for delta encoding
     * @param encodeType    type of page encoding to apply to key
     *
     * @return key serialization to byte array
     */
    byte[] keyEncode(@Nullable K key, @Nonnull K baseKey, @Nonnull PageEncodeType encodeType);

    /**
     * Value encoding.
     *
     * @param value         input to encode
     * @param encodeType    type of page encoding to apply to value
     * @return value serialization to byte array
     */
    byte[] valueEncode(V value, PageEncodeType encodeType);

    /**
     * Unoptimized key encoding. Uses only the state of the
     * byte array to generate the key. The sorted order of
     * two keys should be equal to the natural ordering of
     * their corresponding byte arrays.
     *
     * @param key           serialization of byte array
     * @return deserialized key
     */
    K keyDecode(byte[] key);

    /**
     * Optimized key decoding. Can use the byte array
     * and the base key to generate the key.
     *
     * @param key           serialization of byte array
     * @param baseKey       another key value that can be used for delta decoding
     * @param encodeType    type of page encoding to apply to key
     * @return deserialized key
     */
    K keyDecode(@Nullable byte[] key, @Nonnull K baseKey, @Nonnull PageEncodeType encodeType);

    /**
     * Value decoding.
     *
     * @param value         input to decode
     * @param encodeType    type of page decoding to apply to value
     * @return deserialized value
     */
    V valueDecode(byte[] value, @Nonnull PageEncodeType encodeType);

}
