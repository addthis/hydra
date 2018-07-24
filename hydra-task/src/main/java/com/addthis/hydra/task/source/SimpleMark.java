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
package com.addthis.hydra.task.source;

import com.addthis.basis.util.Varint;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.task.stream.StreamFile;

import com.google.common.base.MoreObjects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;


/** */
public class SimpleMark implements Codable, BytesCodable {

    @FieldConfig(codable = true)
    private String val;
    @FieldConfig(codable = true)
    private long index;
    @FieldConfig(codable = true)
    private boolean end;

    public SimpleMark set(String val, long index) {
        this.setValue(val);
        this.setIndex(index);
        return this;
    }

    public static String calcValue(StreamFile stream) {
        return stream.lastModified() + "/" + stream.length();
    }

    public void update(StreamFile stream) {
        this.setValue(calcValue(stream));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("val", getValue())
                .add("index", getIndex())
                .add("end", isEnd())
                .toString();
    }

    public String getValue() {
        return val;
    }

    public void setValue(String val) {
        this.val = val;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }

    // no-op functions
    public int getError() {
        return -1;
    }

    public void setError(int error) {
    }

    @Override
    public byte[] bytesEncode(long version) {
        byte[] retBytes = null;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            byte[] valBytes = val.getBytes();
            Varint.writeUnsignedVarInt(valBytes.length, buffer);
            buffer.writeBytes(valBytes);
            Varint.writeUnsignedVarLong(index, buffer);
            buffer.writeByte(end ? 1 : 0);
            retBytes = new byte[buffer.readableBytes()];
            buffer.readBytes(retBytes);
        } finally {
            buffer.release();
        }
        return retBytes;
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        ByteBuf buffer = Unpooled.wrappedBuffer(b);
        try {
            int valLength = Varint.readUnsignedVarInt(buffer);
            byte[] valBytes = new byte[valLength];
            buffer.readBytes(valBytes);
            val = new String(valBytes);
            index = Varint.readUnsignedVarLong(buffer);
            end = buffer.readByte() == 1;
        } finally {
            buffer.release();
        }
    }
}
