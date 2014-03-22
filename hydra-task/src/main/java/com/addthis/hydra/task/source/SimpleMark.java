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

import com.addthis.codec.Codec;
import com.addthis.hydra.store.util.Varint;
import com.addthis.hydra.task.stream.StreamFile;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;

/** */
public class SimpleMark implements Codec.Codable, Codec.BytesCodable {

    @Codec.Set(codable = true)
    private String val;
    @Codec.Set(codable = true)
    private long index;
    @Codec.Set(codable = true)
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
        return Objects.toStringHelper(this)
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
    public byte[] bytesEncode() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            byte[] valBytes = val.getBytes();
            Varint.writeUnsignedVarInt(valBytes.length, buffer);
            buffer.writeBytes(valBytes);
            Varint.writeUnsignedVarLong(index, buffer);
            buffer.writeByte(end ? 0 : 1);
            byte[] retBytes = new byte[buffer.readableBytes()];
            buffer.readBytes(retBytes);
            return retBytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void bytesDecode(byte[] b) {
        ByteBuf buffer = Unpooled.wrappedBuffer(b);
        try {
            int valLength = Varint.readUnsignedVarInt(buffer);
            byte[] valBytes = new byte[valLength];
            buffer.readBytes(valBytes);
            val = new String(valBytes);
            index = Varint.readUnsignedVarLong(buffer);
            end = buffer.readByte() == 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
