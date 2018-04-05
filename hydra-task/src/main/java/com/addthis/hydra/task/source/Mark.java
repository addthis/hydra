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

import com.google.common.base.MoreObjects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

/**
 * file mark record
 */
public class Mark extends SimpleMark {

    @FieldConfig(codable = true)
    private int error;

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("value", getValue())
                .add("error", getError())
                .add("index", getIndex())
                .add("end", isEnd())
                .toString();
    }

    @Override public int getError() {
        return error;
    }

    @Override public void setError(int error) {
        this.error = error;
    }

    @Override
    public byte[] bytesEncode(long version) {
        byte[] retBytes = null;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            byte[] valBytes = getValue().getBytes();
            Varint.writeUnsignedVarInt(valBytes.length, buffer);
            buffer.writeBytes(valBytes);
            Varint.writeUnsignedVarLong(getIndex(), buffer);
            buffer.writeByte(isEnd() ? 1 : 0);
            Varint.writeUnsignedVarInt(error, buffer);
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
            setValue(new String(valBytes));
            setIndex(Varint.readUnsignedVarLong(buffer));
            setEnd(buffer.readByte() == 1);
            setError(Varint.readUnsignedVarInt(buffer));
        } finally {
            buffer.release();
        }
    }
}
