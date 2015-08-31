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
package com.addthis.hydra.store.skiplist;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.addthis.basis.util.LessBytes;

import com.addthis.hydra.store.DBIntValue;
import com.addthis.hydra.store.kv.PageEncodeType;
import com.addthis.hydra.store.kv.KeyCoder;

public class SimpleIntKeyCoder implements KeyCoder<Integer, DBIntValue> {

    @Override
    public Integer negInfinity() {
        return Integer.MIN_VALUE;
    }

    @Override
    public byte[] encodedNegInfinity() {
        return keyEncode(negInfinity());
    }

    @Override
    public byte[] keyEncode(Integer key) {
        return key != null ? LessBytes.toBytes(key.intValue() ^ Integer.MIN_VALUE) : new byte[0];
    }

    @Override
    public byte[] keyEncode(@Nullable Integer key, @Nonnull Integer baseKey, @Nonnull PageEncodeType encodeType) {
        if (key == null) {
            return new byte[0];
        }
        return keyEncode(key - baseKey);
    }

    @Override
    public byte[] valueEncode(DBIntValue value, PageEncodeType encodeType) {
        return value.bytesEncode(encodeType.ordinal());
    }

    @Override
    public Integer keyDecode(byte[] key) {
        return (key != null && key.length > 0) ? (LessBytes.toInt(key) ^ Integer.MIN_VALUE) : null;
    }

    @Override
    public Integer keyDecode(@Nullable byte[] key, @Nonnull Integer baseKey, @Nonnull PageEncodeType encodeType) {
        Integer offset = keyDecode(key);
        if (offset == null) {
            return null;
        }
        return offset + baseKey;
    }

    @Override
    public DBIntValue valueDecode(byte[] value, PageEncodeType encodeType) {
        DBIntValue dbIntValue = new DBIntValue();
        dbIntValue.bytesDecode(value, encodeType.ordinal());
        return dbIntValue;
    }

}
