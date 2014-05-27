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

import java.util.Map;

import com.addthis.basis.util.Bytes;

class BytePageEntry implements Map.Entry<byte[], byte[]> {

    private final byte[] key;
    private final byte[] val;

    BytePageEntry(byte[] key, byte[] val) {
        this.key = key;
        this.val = val;
    }

    BytePageEntry(byte[] key) {
        this(key, null);
    }

    @Override
    public String toString() {
        return "PE:" + Bytes.toString(key) + "=" + Bytes.toString(val);
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return val;
    }

    @Override
    public byte[] setValue(byte[] value) {
        throw new UnsupportedOperationException("entry is read only");
    }
}
