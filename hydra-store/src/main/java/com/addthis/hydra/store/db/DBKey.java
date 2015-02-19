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
/**
 *
 */
package com.addthis.hydra.store.db;

import javax.annotation.Nonnull;

import java.util.Arrays;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Varint;

import com.addthis.hydra.store.util.Raw;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


public final class DBKey implements IPageDB.Key, Comparable<DBKey> {

    private static final Raw EMPTY = Raw.get(new byte[0]);

    private final long id;
    private final Raw key;

    public DBKey(long id) {
        this(id, (Raw) null);
    }

    public DBKey(long id, String key) {
        this(id, Raw.get(key));
    }

    public DBKey(long id, Raw key) {
        checkArgument(id >= 0, "Argument was %s but expected non-negative integer", id);
        if (key == null) {
            key = EMPTY;
        }
        this.id = id;
        this.key = key;
    }

    /**
     * Create a DBKey from its serialization byte array representation.
     * If the most significant bit of the first byte is set then
     * interpret the id as a 63-bit unsigned long. Otherwise interpret the
     * id as a 31-bit unsigned int.
     *
     * @param raw
     * @return
     */
    public static DBKey fromBytes(byte[] raw) {
        byte head = raw[0];
        int numBytes;
        long id;
        if ((head >> 7) == 0) {
            id = (long) Bytes.toInt(raw);
            numBytes = 4;
        } else {
            id = Bytes.toLong(raw) & ~(Long.MIN_VALUE);
            numBytes = 8;
        }
        Raw key = Raw.get(Bytes.cut(raw, numBytes, raw.length - numBytes));
        return new DBKey(id, key);
    }

    public static DBKey deltaDecode(byte[] encoding, @Nonnull IPageDB.Key baseKey) {
        ByteBuf buffer = Unpooled.copiedBuffer(encoding);
        long offset = Varint.readSignedVarLong(buffer);
        long id = offset + baseKey.id();
        Raw key;
        if (buffer.readableBytes() == 0) {
            key = null;
        } else {
            byte[] data = new byte[buffer.readableBytes()];
            buffer.readBytes(data);
            key = Raw.get(data);
        }
        return new DBKey(id, key);
    }

    @Override public long id() {
        return id;
    }

    @Override public byte[] key() {
        return key.toBytes();
    }

    @Override public Raw rawKey() {
        return key;
    }

    public String toString() {
        return id + ":" + key;
    }

    @Override
    public int hashCode() {
        return key.hashCode() + Long.hashCode(id);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof DBKey) {
            DBKey k = (DBKey) o;
            return (k.id == id && k.key.equals(key));
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(DBKey dk) {
        long dkId = dk.id();
        if (dkId == id) {
            return key.compareTo(dk.key);
        }
        return id > dkId ? 1 : -1;
    }

    /**
     * Generate the serialized byte array representation.
     * If the id can be represented as a 31-bit unsigned integer
     * then use a 4-byte representation. If is cannot be represented
     * in 4 bytes then use an 8-byte representation and set the
     * most significant bit of the first byte as a flag.
     *
     * @return serialized representation
     */
    @Override public byte[] toBytes() {
        byte[] idBytes;
        if (id <= Integer.MAX_VALUE) {
            idBytes = Bytes.toBytes((int) id);
        } else {
            idBytes = Bytes.toBytes(id | Long.MIN_VALUE);
        }
        if (key == null) {
            return idBytes;
        }
        return Raw.get(idBytes).cat(key).toBytes();
    }

    @Override public byte[] deltaEncode(@Nonnull IPageDB.Key baseKey) {
        long offset = id - baseKey.id();
        ByteBuf buffer = Unpooled.buffer();
        Varint.writeSignedVarLong(offset, buffer);
        if (key != null) {
            buffer.writeBytes(key.toBytes());
        }
        return Arrays.copyOf(buffer.array(), buffer.readableBytes());
    }

}
