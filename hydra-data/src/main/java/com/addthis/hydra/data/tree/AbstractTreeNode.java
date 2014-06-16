package com.addthis.hydra.data.tree;

import java.util.HashMap;

import java.nio.charset.StandardCharsets;

import com.addthis.basis.util.Varint;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public abstract class AbstractTreeNode implements ReadNode, Codec.SuperCodable, Codec.BytesCodable {

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
    protected HashMap<String, TreeNodeData<?>> data;

    @Override
    public void bytesDecode(byte[] b, long version) {
        ByteBuf buf = Unpooled.wrappedBuffer(b);
        try {
            hits = Varint.readUnsignedVarLong(buf);
            nodedb = Varint.readSignedVarInt(buf);
            int numAttachments = Varint.readSignedVarInt(buf);
            if (numAttachments > 0) {
                HashMap<String, TreeNodeData<?>> dataMap = new HashMap<>(numAttachments);
                for (int i = 0; i < numAttachments; i++) {
                    int kl = Varint.readUnsignedVarInt(buf);
                    if (kl == 0) {
                        continue;
                    }
                    String key = new String(buf.readBytes(kl).array(), StandardCharsets.UTF_8);
                    int cl = Varint.readUnsignedVarInt(buf);
                    String className = new String(buf.readBytes(cl).array(), StandardCharsets.UTF_8);
                    TreeNodeData<?> tn = (TreeNodeData) CodecBin2.getClassFieldMap(TreeNodeData.class).getClass(className).newInstance();
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

    @Override
    public void postDecode() {
    }

    @Override
    public void preEncode() {
    }
}
