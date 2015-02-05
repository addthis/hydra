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
package com.addthis.hydra.store.skiplist;

import java.io.DataOutputStream;
import java.io.OutputStream;

import java.util.ArrayList;

import com.addthis.basis.io.GZOut;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Varint;

import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.PageEncodeType;
import com.addthis.hydra.store.kv.TreeEncodeType;

import com.jcraft.jzlib.Deflater;
import com.jcraft.jzlib.DeflaterOutputStream;
import com.ning.compress.lzf.LZFOutputStream;

import org.xerial.snappy.SnappyOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

/**
 * This class is for testing backwards-compatibility only.
 * We no longer encode pages in this format.
 */
@Deprecated
public class SparsePage<K, V extends BytesCodable> extends Page<K, V> {

    protected SparsePage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, encodeType);
    }

    protected SparsePage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys,
                         ArrayList<V> values, ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
    }

    protected static final int FLAGS_IS_SPARSE = 1 << 5;

    public byte[] encode(ByteBufOutputStream out, boolean record) {
        SkipListCacheMetrics metrics = parent.metrics;
        parent.numPagesEncoded.getAndIncrement();
        try {
            OutputStream os = out;
            out.write(gztype | FLAGS_HAS_ESTIMATES | FLAGS_IS_SPARSE);
            switch (gztype) {
                case 0:
                    break;
                case 1:
                    os = new DeflaterOutputStream(out, new Deflater(gzlevel));
                    break;
                case 2:
                    os = new GZOut(out, gzbuf, gzlevel);
                    break;
                case 3:
                    os = new LZFOutputStream(out);
                    break;
                case 4:
                    os = new SnappyOutputStream(out);
                    break;
                default:
                    throw new RuntimeException("invalid gztype: " + gztype);
            }

            DataOutputStream dos = new DataOutputStream(os);
            byte[] firstKeyEncoded = keyCoder.keyEncode(firstKey, TreeEncodeType.BIT32);
            byte[] nextFirstKeyEncoded = keyCoder.keyEncode(nextFirstKey, TreeEncodeType.BIT32);

            updateHistogram(metrics.encodeNextFirstKeySize, nextFirstKeyEncoded.length, record);

            Varint.writeUnsignedVarInt(size, dos);
            Varint.writeUnsignedVarInt(firstKeyEncoded.length, dos);
            dos.write(firstKeyEncoded);
            Varint.writeUnsignedVarInt(nextFirstKeyEncoded.length, dos);
            if (nextFirstKeyEncoded.length > 0) {
                dos.write(nextFirstKeyEncoded);
            }
            for (int i = 0; i < size; i++) {
                byte[] keyEncoded = keyCoder.keyEncode(keys.get(i), TreeEncodeType.BIT32);
                byte[] rawVal = rawValues.get(i);

                if (rawVal == null || encodeType != PageEncodeType.SPARSE) {
                    fetchValue(i);
                    rawVal = keyCoder.valueEncode(values.get(i), PageEncodeType.SPARSE);
                }

                updateHistogram(metrics.encodeKeySize, keyEncoded.length, record);
                updateHistogram(metrics.encodeValueSize, rawVal.length, record);

                Varint.writeUnsignedVarInt(keyEncoded.length, dos);
                dos.write(keyEncoded);
                Varint.writeUnsignedVarInt(rawVal.length, dos);
                dos.write(rawVal);
            }

            Varint.writeUnsignedVarInt((estimateTotal > 0 ? estimateTotal : 1), dos);
            Varint.writeUnsignedVarInt((estimates > 0 ? estimates : 1), dos);
            switch (gztype) {
                case 1:
                    ((DeflaterOutputStream) os).finish();
                    break;
                case 2:
                    ((GZOut) os).finish();
                    break;
            }
            os.flush(); // flush should be called by dos.close(), but better safe than sorry
            dos.close();

            ByteBuf buffer = out.buffer();

            byte[] returnValue = new byte[out.writtenBytes()];

            buffer.readBytes(returnValue);
            buffer.clear();
            updateHistogram(metrics.numberKeysPerPage, size, record);
            updateHistogram(metrics.encodePageSize, returnValue.length, record);
            return returnValue;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class SparsePageFactory<K, V extends BytesCodable> extends PageFactory<K,V> {

        public static final SparsePageFactory singleton = new SparsePageFactory();

        private SparsePageFactory() {}

        @Override
        public Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType) {
            return new SparsePage(cache, firstKey, nextFirstKey, PageEncodeType.SPARSE);
        }

        @Override
        public Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys,
                ArrayList<V> values, ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
            return new SparsePage(cache, firstKey, nextFirstKey, size, keys, values, rawValues, PageEncodeType.SPARSE);
        }

        @Override
        public TreeEncodeType defaultEncodeType() {
            return PageEncodeType.SPARSE.getTreeType();
        }
    }

}
