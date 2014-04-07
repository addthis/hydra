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

import com.addthis.basis.util.Bytes;

import com.addthis.hydra.store.DBIntValue;
import com.addthis.hydra.store.kv.KeyCoder;


public class SimpleIntKeyCoder implements KeyCoder<Integer, DBIntValue> {

    @Override
    public Integer negInfinity() {
        return Integer.MIN_VALUE;
    }

    @Override
    public byte[] keyEncode(Integer key) {
        return key != null ? Bytes.toBytes(key.intValue() ^ Integer.MIN_VALUE) : new byte[0];
    }

    @Override
    public byte[] valueEncode(DBIntValue value, EncodeType encodeType) {
        return value.bytesEncode(encodeType.ordinal());
    }

    @Override
    public Integer keyDecode(byte[] key) {
        return (key != null && key.length > 0) ? (Bytes.toInt(key) ^ Integer.MIN_VALUE) : null;
    }

    @Override
    public DBIntValue valueDecode(byte[] value, EncodeType encodeType) {
        DBIntValue dbIntValue = new DBIntValue();
        dbIntValue.bytesDecode(value, encodeType.ordinal());
        return dbIntValue;
    }

    @Override
    public boolean nullRawValueInternal(byte[] value) {
        return value.length == 0;
    }

}
