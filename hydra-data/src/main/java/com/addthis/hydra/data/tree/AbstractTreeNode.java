package com.addthis.hydra.data.tree;

import com.addthis.basis.util.Varint;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractTreeNode implements DataTreeNode, Codec.SuperCodable, Codec.ConcurrentCodable, Codec.BytesCodable {

    @Codec.Set(codable = true)
    protected long hits;
    @Codec.Set(codable = true)
    protected int nodes;
    @Codec.Set(codable = true)
    protected volatile Integer nodedb;
    @Codec.Set(codable = true)
    protected int bits;
    @SuppressWarnings("unchecked")
    @Codec.Set(codable = true)
    protected HashMap<String, TreeNodeData> data;


    @Override
    public byte[] bytesEncode(long version) {
        preEncode();
        if (!encodeLock()) {
            throw new RuntimeException("Unable to acquire encoding lock");
        }
        byte[] returnBytes;
        ByteBuf b = PooledByteBufAllocator.DEFAULT.buffer();
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
                    String classInfo = CodecBin2.getClassFieldMap(entry.getValue().getClass()).getClassName(entry.getValue());
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
                    TreeNodeData tn = (TreeNodeData) CodecBin2.getClassFieldMap(TreeNodeData.class).getClass(className).newInstance();
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
