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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Varint;

import io.netty.buffer.ByteBuf;

public enum PageEncodeType {
    LEGACY,
    SPARSE,
    LONGIDS;

    /**
     * Reads the next integer from the input stream.
     *
     * @param in   input stream
     * @param dis  possible data input stream wrapping the input stream
     * @return integer value
     * @throws IOException
     */
    public int readInt(@Nullable InputStream in, @Nullable DataInputStream dis) throws IOException {
        switch (this) {
            case LEGACY:
                assert in != null;
                return (int) Bytes.readLength(in);
            case SPARSE:
            case LONGIDS:
                assert dis != null;
                return Varint.readUnsignedVarInt(dis);
            default:
                throw new IllegalStateException("unknown state " + this);
        }
    }

    /**
     * Reads the next byte array from input stream.
     * @param in  input stream
     * @param dis possible data input stream wrapping the input stream
     * @return byte array
     * @throws IOException
     */
    public byte[] readBytes(@Nonnull InputStream in, @Nullable DataInputStream dis) throws IOException {
        switch (this) {
            case LEGACY:
                return Bytes.readBytes(in);
            case SPARSE:
            case LONGIDS:
                assert dis != null;
                return Bytes.readBytes(in, Varint.readUnsignedVarInt(dis));
            default:
                throw new IllegalStateException("unknown state " + this);
        }
    }

    /**
     * Retrieves the next first key from a page.
     * @param in  input stream
     * @param dis possible data input stream wrapping the input stream
     * @return encoded next first key
     * @throws IOException
     */
    public byte[] nextFirstKey(@Nonnull InputStream in, @Nullable DataInputStream dis) throws IOException {
        switch (this) {
            case LEGACY:
                return Bytes.readBytes(in);
            case SPARSE:
            case LONGIDS: {
                assert dis != null;
                int nextFirstKeyLength = Varint.readUnsignedVarInt(dis);
                if (nextFirstKeyLength > 0) {
                    return Bytes.readBytes(in, nextFirstKeyLength);
                } else {
                    return null;
                }
            }
            default:
                throw new IllegalStateException("unknown state " + this);
        }
    }

    /**
     * Read a node identifier that is encoded in the values of a page.
     *
     * @param buf
     * @param version
     * @return
     */
    public static long readNodeId(ByteBuf buf, long version) {
        if (version <= SPARSE.ordinal()) {
            return (long) Varint.readSignedVarInt(buf);
        } else {
            return Varint.readSignedVarLong(buf);
        }
    }

    /**
     * Write a node identifer that is encoded in the values of a page.
     *
     * @param buf
     * @param version
     * @param nodedb
     */
    public static void writeNodeId(ByteBuf buf, long version, Long nodedb) {
        if (version <= PageEncodeType.SPARSE.ordinal()) {
            Varint.writeSignedVarInt(nodedb == null ? -1 : nodedb.intValue(), buf);
        } else {
            Varint.writeSignedVarLong(nodedb == null ? -1l : nodedb, buf);
        }
    }

    public static PageEncodeType defaultType() {
        return LONGIDS;
    }

}