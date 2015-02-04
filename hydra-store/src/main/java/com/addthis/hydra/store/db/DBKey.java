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

import com.addthis.hydra.store.kv.TreeEncodeType;
import com.addthis.hydra.store.util.Raw;

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
        if (key == null) {
            key = EMPTY;
        }
        this.id = id;
        this.key = key;
    }

    public static DBKey fromBytes(byte[] raw, TreeEncodeType encodeType) {
        int numBytes = encodeType.getBytes();
        if (raw == null) {
            throw new NullPointerException("input array to DBKey must be non-null");
        } else if (raw.length < numBytes) {
            throw new IllegalStateException("input array to DBKey has length "
                                            + raw.length + " and must have at least length " + encodeType.getBytes());
        }
        long id = encodeType.getId(raw, 0, -1);
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

    @Override public byte[] toBytes(@Nonnull TreeEncodeType encodeType) {
        byte[] idBytes  = encodeType.idToBytes(id);
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
