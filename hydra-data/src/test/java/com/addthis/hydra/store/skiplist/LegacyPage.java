package com.addthis.hydra.store.skiplist;

import java.io.OutputStream;

import java.util.ArrayList;

import com.addthis.basis.io.GZOut;
import com.addthis.basis.util.Bytes;

import com.addthis.codec.Codec;
import com.addthis.hydra.store.kv.KeyCoder;

import com.jcraft.jzlib.Deflater;
import com.jcraft.jzlib.DeflaterOutputStream;
import com.ning.compress.lzf.LZFOutputStream;

import org.xerial.snappy.SnappyOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

public class LegacyPage<K, V extends Codec.BytesCodable> extends Page<K, V> {

    protected LegacyPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, KeyCoder.EncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, encodeType);
    }

    protected LegacyPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys,
            ArrayList<V> values, ArrayList<byte[]> rawValues, KeyCoder.EncodeType encodeType) {
        super(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
    }

    @Override
    public byte[] encode(ByteBufOutputStream out, boolean record) {
        SkipListCacheMetrics metrics = parent.metrics;
        parent.numPagesEncoded.getAndIncrement();
        try {
            OutputStream os = out;
            out.write(gztype | FLAGS_HAS_ESTIMATES);
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

            byte[] firstKeyEncoded = keyCoder.keyEncode(firstKey);
            byte[] nextFirstKeyEncoded = keyCoder.keyEncode(nextFirstKey);

            updateHistogram(metrics.encodeFirstKeySize, firstKeyEncoded.length, record);
            updateHistogram(metrics.encodeNextFirstKeySize, nextFirstKeyEncoded.length, record);

            Bytes.writeLength(size, os);
            Bytes.writeBytes(firstKeyEncoded, os);
            Bytes.writeBytes(nextFirstKeyEncoded, os);
            for (int i = 0; i < size; i++) {
                byte[] keyEncoded = keyCoder.keyEncode(keys.get(i));
                byte[] rawVal = rawValues.get(i);

                if (rawVal == null) {
                    rawVal = keyCoder.valueEncode(values.get(i), KeyCoder.EncodeType.LEGACY);
                }

                updateHistogram(metrics.encodeKeySize, keyEncoded.length, record);
                updateHistogram(metrics.encodeValueSize, rawVal.length, record);

                Bytes.writeBytes(keyEncoded, os);
                Bytes.writeBytes(rawVal, os);
            }
            Bytes.writeLength((estimateTotal > 0 ? estimateTotal : 1), os);
            Bytes.writeLength((estimates > 0 ? estimates : 1), os);
            switch (gztype) {
                case 1:
                    ((DeflaterOutputStream) os).finish();
                    break;
                case 2:
                    ((GZOut) os).finish();
                    break;
                case 4:
                    os.flush();
                    break;
            }
            os.flush();
            os.close();

            ByteBuf buffer = out.buffer();

            byte[] returnValue = new byte[out.writtenBytes()];

            buffer.readBytes(returnValue);
            buffer.clear();

            updateHistogram(metrics.numberKeysPerPage, size, record);
            updateHistogram(metrics.encodePageSize, returnValue.length, record);
            return returnValue;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class LegacyPageFactory<K, V extends Codec.BytesCodable> extends PageFactory<K,V> {

        public static final LegacyPageFactory singleton = new LegacyPageFactory();

        private LegacyPageFactory() {}

        @Override
        public Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, KeyCoder.EncodeType encodeType) {
            return new LegacyPage(cache, firstKey, nextFirstKey, KeyCoder.EncodeType.LEGACY);
        }

        @Override
        public Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys,
                ArrayList<V> values, ArrayList<byte[]> rawValues, KeyCoder.EncodeType encodeType) {
            return new LegacyPage(cache, firstKey, nextFirstKey, size, keys, values, rawValues, KeyCoder.EncodeType.LEGACY);
        }
    }

}
