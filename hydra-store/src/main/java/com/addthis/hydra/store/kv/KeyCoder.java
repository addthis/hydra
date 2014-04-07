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


public interface KeyCoder<K, V> {

    enum EncodeType {LEGACY, SPARSE}

    K negInfinity();

    byte[] keyEncode(K key);

    byte[] valueEncode(V value, EncodeType encodeType);

    K keyDecode(byte[] key);

    V valueDecode(byte[] value, EncodeType encodeType);

    /**
     * throws a NullPointerException if the input is null.
     *
     * @param value a non-null value
     * @return true if-and-only-if the input encodes the null value.
     */
    boolean nullRawValueInternal(byte[] value);
}
