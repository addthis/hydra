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

import com.addthis.basis.util.Bytes;

class SimpleStringKeyCoder implements KeyCoder<String, String> {

    @Override
    public String negInfinity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] keyEncode(String key) {
        return key != null ? Bytes.toBytes(key) : new byte[0];
    }

    @Override
    public byte[] valueEncode(String value) {
        return keyEncode(value);
    }

    @Override
    public String keyDecode(byte[] key) {
        return key.length > 0 ? Bytes.toString(key) : null;
    }

    @Override
    public String valueDecode(byte[] value) {
        return keyDecode(value);
    }

    @Override
    public boolean nullRawValueInternal(byte[] value) {
        throw new UnsupportedOperationException();
    }

}
