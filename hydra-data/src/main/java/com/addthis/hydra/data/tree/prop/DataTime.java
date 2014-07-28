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

import com.addthis.basis.util.Varint;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeUpdater;
import com.addthis.hydra.data.tree.TreeDataParameters;
import com.addthis.hydra.data.tree.TreeNodeData;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public class DataTime extends TreeNodeData<DataTime.Config> {

    /**
     * This data attachment <span class="hydra-summary">stores the lowest and
     * highest values for a specified field</span>.
     * <p/>
     * <p>Job Configuration Example:</p>
     * <pre>
     * { type : "value", key : "ID", data : {
     *   time : { type : "time", key : "TIME"},
     * }},</pre>
     *
     * <p><b>Query Path Directives</b>
     *
     * <p>${attachment}=first returns the lowest value.
     * <p>${attachment}=last returns the highest value.
     * <p>${attachment}=life returns the highest value minus the lowest value.
     *
     * <p>% operations are not supported</p>
     *
     * <p>Query Path Example:</p>
     * <pre>
     *     /+$+time=life
     * </pre>
     *
     * @user-reference
     * @hydra-name time
     */
    public static final class Config extends TreeDataParameters<DataTime> {

        /**
         * Bundle field name from which to draw values.
         * This field is required.
         */
        @FieldConfig(codable = true, required = true)
        private String key;

        @Override
        public DataTime newInstance() {
            return new DataTime();
        }
    }

    @FieldConfig(codable = true)
    private long first = Long.MAX_VALUE;
    @FieldConfig(codable = true)
    private long last = Long.MIN_VALUE;

    private BundleField keyAccess;

    public long first() {
        return first;
    }

    public long last() {
        return last;
    }

    @Override
    public boolean updateChildData(DataTreeNodeUpdater state, DataTreeNode tn, Config conf) {
        Bundle p = state.getBundle();
        if (keyAccess == null) {
            keyAccess = p.getFormat().getField(conf.key);
        }
        long packetTime = ValueUtil.asNumber(p.getValue(keyAccess)).asLong().getLong();
        first = Math.min(first, packetTime);
        last = Math.max(last, packetTime);
        return true;
    }

    @Override
    public ValueObject getValue(String key) {
        if (key == null) {
            return null;
        } else if (key.equals("first")) {
            return ValueFactory.create(first);
        } else if (key.equals("last")) {
            return ValueFactory.create(last);
        } else if (key.equals("life")) {
            return ValueFactory.create(last - first);
        } else {
            return null;
        }
    }

    @Override
    public byte[] bytesEncode(long version) {
        byte[] encodedBytes = null;
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            long delta = last - first;
            Varint.writeUnsignedVarLong(first, byteBuf);
            Varint.writeUnsignedVarLong(delta, byteBuf);
            encodedBytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(encodedBytes);
        } finally {
            byteBuf.release();
        }
        return encodedBytes;
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(b);
        try {
            first = Varint.readUnsignedVarLong(byteBuf);
            last = first + Varint.readUnsignedVarLong(byteBuf);
        } finally {
            byteBuf.release();
        }
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public void setLast(long last) {
        this.last = last;
    }
}
