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

import com.addthis.basis.util.LessBytes;

import com.addthis.hydra.store.DBValue;

class SimpleStringKeyCoder implements KeyCoder<String, DBValue> {

    @Override
    public String negInfinity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] keyEncode(String key) {
        return key != null ? LessBytes.toBytes(key) : new byte[0];
    }

    @Override public byte[] keyEncode(@Nullable String key, @Nonnull String baseKey, @Nonnull PageEncodeType encodeType) {
        return keyEncode(key);
    }

    @Override
    public byte[] valueEncode(DBValue value, PageEncodeType encodeType) {
        return value.bytesEncode(encodeType.ordinal());
    }

    @Override
    public String keyDecode(byte[] key) {
        return key.length > 0 ? LessBytes.toString(key) : null;
    }

    @Override public String keyDecode(@Nullable byte[] key, @Nonnull String baseKey, @Nonnull PageEncodeType encodeType) {
        return keyDecode(key);
    }

    @Override
    public DBValue valueDecode(byte[] value, PageEncodeType encodeType) {
        return new DBValue(new String(value));
    }

}
