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
package com.addthis.hydra.data.tree.prop;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.addthis.basis.collect.HotMap;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;
import com.addthis.basis.util.Varint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class DataMap extends TreeNodeData<DataMap.Config> implements Codec.SuperCodable {

    static final boolean IGNORE_DESERIALIZATION_ERROR = Boolean.getBoolean("hydra.tree.data.map");

    /**
     * <p><span class="hydra-summary">maintains a KV map maintained as an LRU up to a given size</span>.
     * That is, new elements added to the map that would increase its size over the max are added but the element
     * of least interest based on last access time are deleted.
     * <p/>
     * <p>${attachment}={key} will return what (if anything) the map has stored for that key
     * <p/>
     * <p>%{attachment}={comma sep'd list of keys}[/+] will return all matching keys with a virtual node for each that represents
     * the stored value for that key. The /+ is what includes the virtual layer of child nodes containing the values.</p>
     *
     * @user-reference
     * @hydra-name map
     */
    public static final class Config extends TreeDataParameters<DataMap> {

        /**
         * Field to get the map key from. This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String key;

        /**
         * Field to get the mapped value from. This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private String val;

        /**
         * Size of the map. When a new key would be placed into the map, and it would put the size over this value,
         * the oldest entry is evicted. This field is required.
         */
        @Codec.Set(codable = true, required = true)
        private Integer size;

        @Override
        public DataMap newInstance() {
            DataMap top = new DataMap();
            top.size = size;
            return top;
        }
    }

    public DataMap() {
    }

    public DataMap(int size) {
        this.size = size;
    }

    @Override
    public void postDecode() {
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], ValueFactory.create(vals[i]));
        }
    }

    /**
     * The temporary variables are used because it is possible
     * for concurrent serialization threads to be encoding the object.
     * The tree node that contains this data attachment is protected
     * by a reader lock for the encoding process.
     */
    @Override
    public void preEncode() {
        String[] newKeys = new String[map.size()];
        String[] newVals = new String[map.size()];
        int pos = 0;
        for (Entry<String, ValueObject> next : map) {
            newKeys[pos] = next.getKey();
            newVals[pos++] = next.getValue().toString();
        }
        keys = newKeys;
        vals = newVals;
    }

    @Codec.Set(codable = true, required = true)
    private String[] keys;
    @Codec.Set(codable = true, required = true)
    private String[] vals;
    @Codec.Set(codable = true, required = true)
    private int size;

    private BundleField keyAccess;
    private BundleField valAccess;
    private HotMap<String, ValueObject> map = new HotMap<>(new HashMap());

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode childNode, DataMap.Config conf) {
        if (keyAccess == null) {
            keyAccess = state.getBundle().getFormat().getField(conf.key);
            valAccess = state.getBundle().getFormat().getField(conf.val);
        }
        ValueObject key = state.getBundle().getValue(keyAccess);
        ValueObject val = state.getBundle().getValue(valAccess);
        if (key != null) {
            put(key.toString(), val);
            return true;
        } else {
            return false;
        }
    }

    public void put(String key, ValueObject val) {
        synchronized (map) {
            if (val == null) {
                map.remove(key);
            } else {
                map.put(key, val);
                if (map.size() > size) {
                    map.removeEldest();
                }
            }
        }
    }

    @Override
    public ValueObject getValue(String key) {
        synchronized (map) {
            return map.get(key);
        }
    }

    /**
     * return types of synthetic nodes returned
     */
    public List<String> getNodeTypes() {
        return Arrays.asList(new String[]{"#"});
    }

    @Override
    public List<DataTreeNode> getNodes(DataTreeNode parent, String key) {
        String[] keys = key != null ? Strings.splitArray(key, ",") : null;
        ArrayList<DataTreeNode> list = new ArrayList<>(map.size());
        synchronized (map) {
            if (keys != null && keys.length > 0) {
                for (String k : keys) {
                    ValueObject val = map.get(k);
                    if (val != null) {
                        VirtualTreeNode child = new VirtualTreeNode(val.toString(), 1);
                        VirtualTreeNode vtn = new VirtualTreeNode(k, 1, new VirtualTreeNode[]{child});
                        list.add(vtn);
                    }
                }
            } else {
                for (Entry<String, ValueObject> e : map) {
                    VirtualTreeNode child = new VirtualTreeNode(e.getValue().toString(), 1);
                    VirtualTreeNode vtn = new VirtualTreeNode(e.getKey(), 1, new VirtualTreeNode[]{child});
                    list.add(vtn);
                }
            }
        }
        return list;
    }

    @Override
    public byte[] bytesEncode(long version) {
        byte[] encodedBytes = null;
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            synchronized (map) {
                preEncode();
                Varint.writeUnsignedVarInt(keys.length, buf);
                for (String key : keys) {
                    writeString(buf, key);
                }
                for (String val : vals) {
                    writeString(buf, val);
                }
                Varint.writeUnsignedVarInt(size, buf);
            }
            encodedBytes = new byte[buf.readableBytes()];
            buf.readBytes(encodedBytes);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            buf.release();
        }
        return encodedBytes;
    }

    private void writeString(ByteBuf buf, String str) throws UnsupportedEncodingException {
        byte[] keyBytes = str.getBytes("UTF-8");
        Varint.writeUnsignedVarInt(keyBytes.length, buf);
        buf.writeBytes(keyBytes);
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        ByteBuf buf = Unpooled.wrappedBuffer(b);
        try {
            int length = Varint.readUnsignedVarInt(buf);
            keys = new String[length];
            vals = new String[length];
            try {
                for (int i = 0; i < length; i++) {
                    keys[i] = readString(buf);
                }
                for (int i = 0; i < length; i++) {
                    vals[i] = readString(buf);
                }
                if (buf.readableBytes() > 0) {
                    size = Varint.readUnsignedVarInt(buf);
                } else {
                    if (!IGNORE_DESERIALIZATION_ERROR) {
                        throw new RuntimeException("Tried to deserialize a corrupted DataMap attachment. " +
                        "set the system property hydra.tree.data.map=true to ignore (Map Attachment will be empty on old nodes)");
                    }
                }
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        } finally {
            buf.release();
        }
        postDecode();
    }

    private String readString(ByteBuf buf) throws UnsupportedEncodingException {
        int kl = Varint.readUnsignedVarInt(buf);
        byte[] kb = new byte[kl];
        buf.readBytes(kb);
        return new String(kb, "UTF-8");
    }
}
