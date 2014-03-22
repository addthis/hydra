package com.addthis.hydra.data.tree;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import com.addthis.hydra.store.util.Varint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
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
    public byte[] bytesEncode() {
        preEncode();
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();

        if (!encodeLock()) {
            throw new RuntimeException("Unable to acquire encoding lock");
        }
        try {
            Varint.writeUnsignedVarLong(hits, b);
            Varint.writeUnsignedVarInt(nodes, b);
            if (nodedb == null) {
                b.writeByte(0);
            } else {
                b.writeByte(1);
                Varint.writeUnsignedVarInt(nodedb, b);
            }
            Varint.writeUnsignedVarInt(bits, b);
            int numAttachments = data == null ? 0 : data.size();
            Varint.writeUnsignedVarInt(numAttachments, b);

            if (data != null) {
                for (Map.Entry<String, TreeNodeData> entry : data.entrySet()) {

                    byte[] keyBytes = entry.getKey().getBytes(Charset.forName("UTF-8"));
                    Varint.writeUnsignedVarLong(keyBytes.length, b);
                    b.writeBytes(keyBytes);
                    String classInfo = CodecBin2.getClassFieldMap(entry.getValue().getClass()).getClassName(entry.getValue());
                    byte[] classNameBytes = classInfo.getBytes(Charset.forName("UTF-8"));
                    Varint.writeUnsignedVarLong(classNameBytes.length, b);
                    b.writeBytes(classNameBytes);
                    byte[] bytes = CodecBin2.encodeBytes(entry.getValue());
                    Varint.writeUnsignedVarLong(bytes.length, b);
                    b.writeBytes(bytes);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            encodeUnlock();
        }
        byte[] bytes = new byte[b.readableBytes()];
        b.readBytes(bytes);
        return bytes;
    }

    @Override
    public void bytesDecode(byte[] b) {
        ByteBuf buf = Unpooled.copiedBuffer(b);
        try {
            hits = Varint.readUnsignedVarLong(buf);
            nodes = Varint.readUnsignedVarInt(buf);
            boolean hasNodeDB = buf.readByte() == 1;
            if (hasNodeDB) {
                nodedb = Varint.readUnsignedVarInt(buf);
            } else {
                nodedb = null;
            }
            bits = Varint.readUnsignedVarInt(buf);
            int numAttachments = Varint.readUnsignedVarInt(buf);
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
                    Class<?> vc = CodecBin2.getClassFieldMap(TreeNodeData.class).getClass(className);
                    int vl = Varint.readUnsignedVarInt(buf);
                    TreeNodeData tn = (TreeNodeData) CodecBin2.decodeBytes(vc.newInstance(), buf.readBytes(vl).array());
                    dataMap.put(key, tn);

                }
                data = dataMap;
            }
            postDecode();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
