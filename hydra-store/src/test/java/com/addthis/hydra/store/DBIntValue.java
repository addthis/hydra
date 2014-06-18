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
package com.addthis.hydra.store;

import com.addthis.codec.Codec;

import java.nio.ByteBuffer;

public class DBIntValue implements Codec.BytesCodable {

    private Integer val;

    public DBIntValue(Integer val) {
        this.val = val;
    }

    public DBIntValue() {
    }

    public Integer getVal() {
        return val;
    }

    @Override
    public byte[] bytesEncode(long version) {
        return ByteBuffer.allocate(4).putInt(val).array();
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        val = b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DBIntValue that = (DBIntValue) o;

        if (val != null ? !val.equals(that.val) : that.val != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return val != null ? val.hashCode() : 0;
    }
}
