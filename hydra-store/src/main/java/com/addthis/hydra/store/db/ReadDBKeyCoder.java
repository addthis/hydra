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
package com.addthis.hydra.store.db;

import com.addthis.codec.Codec;

/**
 */
public class ReadDBKeyCoder<V extends IReadWeighable & Codec.BytesCodable> extends DBKeyCoder<V> {

    public ReadDBKeyCoder(Codec codec, Class<? extends V> clazz) {
        super(codec, clazz);
    }

    public ReadDBKeyCoder(Class<? extends V> clazz) {
        super(clazz);
    }

    @Override
    public V valueDecode(byte[] value, EncodeType encodeType) {
        V val = super.valueDecode(value, encodeType);
        val.setWeight(value.length);
        return val;
    }
}
