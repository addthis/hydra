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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;

import com.addthis.hydra.store.util.Raw;


public final class DBKey implements IPageDB.Key, Comparable<DBKey> {

    private static final Raw empty = Raw.get(new byte[0]);

    private final int id;
    private final Raw key;

    public DBKey(byte raw[]) {
        id = Bytes.toInt(raw, 0, 4);
        key = Raw.get(Bytes.cut(raw, 4, raw.length - 4));
    }

    public DBKey(InputStream in) throws IOException {
        this(Bytes.readInt(in), Raw.get(Bytes.readBytes(in)));
    }

    public DBKey(int id) {
        this(id, (Raw) null);
    }

    public DBKey(int id, String key) {
        this(id, Raw.get(key));
    }

    public DBKey(int id, Raw key) {
        if (key == null) {
            key = empty;
        }
        this.id = id;
        this.key = key;
    }

    public int id() {
        return id;
    }

    public byte[] key() {
        return key.toBytes();
    }

    public Raw rawKey() {
        return key;
    }

    public byte[] toBytes() {
        if (key == null) {
            return Bytes.toBytes(id);
        }
        return Raw.get(Bytes.toBytes(id)).cat(key).toBytes();
    }

    public void writeOut(OutputStream out) throws IOException {
        Bytes.writeInt(id, out);
        Bytes.writeBytes(key.toBytes(), out);
    }

    public String toString() {
        return id + ":" + key;
    }

    @Override
    public int hashCode() {
        return key.hashCode() + id;
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
        int dkId = dk.id();
        if (dkId == id) {
            return key.compareTo(dk.key);
        }
        return id > dkId ? 1 : -1;
    }
}
