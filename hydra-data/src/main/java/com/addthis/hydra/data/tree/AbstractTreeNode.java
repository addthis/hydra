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
package com.addthis.hydra.data.tree;

import java.util.HashMap;
import java.util.Map;

import java.nio.charset.Charset;

import com.addthis.basis.util.Varint;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.codec.codables.ConcurrentCodable;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.codec.reflection.Fields;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public abstract class AbstractTreeNode implements DataTreeNode, SuperCodable, ConcurrentCodable,
                                                  BytesCodable {

    @FieldConfig(codable = true)
    protected long hits;
    @FieldConfig(codable = true)
    protected int nodes;
    @FieldConfig(codable = true)
    protected volatile Integer nodedb;
    @FieldConfig(codable = true)
    protected int bits;
    @SuppressWarnings("unchecked")
    @FieldConfig(codable = true)
    protected HashMap<String, TreeNodeData> data;


    @Override
    public byte[] bytesEncode(long version) {
        preEncode();
        byte[] returnBytes;
        ByteBuf b = PooledByteBufAllocator.DEFAULT.buffer();
        encodeLock();
        try {
            Varint.writeUnsignedVarLong(hits, b);
            Varint.writeSignedVarInt(nodedb == null ? -1 : nodedb, b);
            if (data != null && data.size() > 0) {
                int numAttachments = data.size();
                Varint.writeSignedVarInt(numAttachments, b);
                for (Map.Entry<String, TreeNodeData> entry : data.entrySet()) {

                    byte[] keyBytes = entry.getKey().getBytes(Charset.forName("UTF-8"));
                    Varint.writeUnsignedVarInt(keyBytes.length, b);
                    b.writeBytes(keyBytes);
                    String classInfo = Fields.getClassFieldMap(entry.getValue().getClass()).getClassName(entry.getValue());
                    byte[] classNameBytes = classInfo.getBytes(Charset.forName("UTF-8"));
                    Varint.writeUnsignedVarInt(classNameBytes.length, b);
                    b.writeBytes(classNameBytes);
                    byte[] bytes = entry.getValue().bytesEncode(version);
                    Varint.writeUnsignedVarInt(bytes.length, b);
                    b.writeBytes(bytes);
                }
            } else {
                Varint.writeSignedVarInt(-1, b);
            }
            if (nodedb != null) {
                Varint.writeUnsignedVarInt(nodes, b);
                Varint.writeUnsignedVarInt(bits, b);
            }
            returnBytes = new byte[b.readableBytes()];
            b.readBytes(returnBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            b.release();
            encodeUnlock();
        }
        return returnBytes;
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        ByteBuf buf = Unpooled.wrappedBuffer(b);
        try {
            hits = Varint.readUnsignedVarLong(buf);
            nodedb = Varint.readSignedVarInt(buf);
            int numAttachments = Varint.readSignedVarInt(buf);
            if (numAttachments > 0) {
                HashMap<String, TreeNodeData> dataMap = new HashMap<>();
                for (int i = 0; i < numAttachments; i++) {
                    int kl = Varint.readUnsignedVarInt(buf);
                    if (kl == 0) {
                        continue;
                    }
                    String key = new String(buf.readBytes(kl).array(), Charset.forName("UTF-8"));
                    int cl = Varint.readUnsignedVarInt(buf);
                    String className = new String(buf.readBytes(cl).array(), Charset.forName("UTF-8"));
                    TreeNodeData tn = (TreeNodeData) Fields.getClassFieldMap(TreeNodeData.class).getClass(className).newInstance();
                    int vl = Varint.readUnsignedVarInt(buf);
                    tn.bytesDecode(buf.readBytes(vl).array(), version);
                    dataMap.put(key, tn);
                }
                data = dataMap;
            }
            if (nodedb > 0) {
                nodes = Varint.readUnsignedVarInt(buf);
                bits = Varint.readUnsignedVarInt(buf);
            } else {
                nodedb = null;
            }
            postDecode();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            buf.release();
        }
    }
}
