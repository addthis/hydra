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

    /**
     * LEGACY - all serialization is performed using CodecBin2
     * SPARSE - initial implementation of BytesCodable
     * KEYTOPPER - BytesCodable added to DataLimitHot, DataLimitTop, and DataTop
     */
    enum EncodeType {
        LEGACY, SPARSE, KEYTOPPER;

        public static EncodeType from(int ordinal) {
            switch(ordinal) {
                case 0:
                    return LEGACY;
                case 1:
                    return SPARSE;
                case 2:
                    return KEYTOPPER;
                default:
                    throw new IllegalStateException("Unknown ordinal value " + ordinal);
            }
        }

        public static EncodeType defaultType() {
            return KEYTOPPER;
        }

        public static int defaultOrdinal() {
            return defaultType().ordinal();
        }
    }

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
