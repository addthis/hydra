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

import java.io.File;
import java.io.IOException;

import java.util.Map;
import java.util.TreeMap;

import com.addthis.basis.util.Varint;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.db.DBKey;
import com.addthis.hydra.store.db.ReadPageDB;
import com.addthis.hydra.store.kv.IReadWeighable;
import com.addthis.hydra.store.kv.ReadPageCache;
import com.addthis.hydra.store.kv.ReadPageCaches;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;


public class MarkDB {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("MarkDB called without directory. Trying using the script in hydra-config/tools.");
            return;
        }
        File markDirFile = new File(args[0]);
        if (args.length == 2 && args[1].equals("-s")) {
            try {
                ReadPageCache<DBKey, Record> markDB = ReadPageDB.newPageCache(markDirFile, Record.class, 1000, 20);
                TreeMap<DBKey, Record> tm = ReadPageCaches.toTreeMap(markDB);
                for (Map.Entry<DBKey, Record> se : tm.entrySet()) {
                    System.out.println("Path: " + se.getKey().rawKey() + " Index: " + se.getValue().index);
                }
                markDB.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to open database. Here is a stack trace.");
            }
        } else {
            try {
                ReadPageCache<DBKey, Mark> markDB = ReadPageDB.newPageCache(markDirFile, Mark.class, 1000, 20);
                TreeMap<DBKey, Mark> tm = ReadPageCaches.toTreeMap(markDB);
                for (Map.Entry<DBKey, Mark> se : tm.entrySet()) {
                    System.out.println("Path: " + se.getKey().rawKey() + " Index: " + se.getValue().index);
                }
                markDB.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to open database. Here is a stack trace.");
            }
        }
    }

    public static final class Record implements Codec.Codable, IReadWeighable, Codec.BytesCodable {

        public String val;
        public long index;
        public boolean end;

        private int weight = 0;

        @Override
        public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override
        public int getWeight() {
            return weight;
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
                end = (buffer.readByte() == 1);
            } finally {
                buffer.release();
            }
        }

    }

    public static final class Mark implements Codec.Codable, Codec.BytesCodable, IReadWeighable {

        public String value;
        public long index;
        public int error;
        public boolean end;

        private int weight = 0;

        @Override
        public void setWeight(int weight) {
            this.weight = weight;
        }

        @Override
        public int getWeight() {
            return weight;
        }

        @Override
        public byte[] bytesEncode(long version) {
            byte[] retBytes = null;
            ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer();
            try {
                byte[] valBytes = value.getBytes();
                Varint.writeUnsignedVarInt(valBytes.length, buffer);
                buffer.writeBytes(valBytes);
                Varint.writeUnsignedVarLong(index, buffer);
                buffer.writeByte(end ? 1 : 0);
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
                value = new String(valBytes);
                index = Varint.readUnsignedVarLong(buffer);
                end = (buffer.readByte() == 1);
                error = Varint.readUnsignedVarInt(buffer);
            } finally {
                buffer.release();
            }
        }

    }
}
