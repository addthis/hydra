package com.addthis.hydra.store.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Map;
import java.util.zip.GZIPInputStream;

import com.addthis.basis.io.GZOut;
import com.addthis.basis.util.Bytes;

import com.addthis.codec.Codec;

import com.jcraft.jzlib.Deflater;
import com.jcraft.jzlib.DeflaterOutputStream;
import com.jcraft.jzlib.InflaterInputStream;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class LegacyExternalPagedStore<K extends Comparable<K>, V extends Codec.BytesCodable>
        extends ExternalPagedStore<K,V> {

    public LegacyExternalPagedStore(KeyCoder<K, V> keyCoder, ByteStore pages) {
        super(keyCoder, pages);
    }

    public LegacyExternalPagedStore(KeyCoder<K, V> keyCoder, ByteStore pages, int maxPageSize, int maxPages) {
        super(keyCoder, pages, maxPageSize, maxPages);
    }

    private static final Logger log = LoggerFactory.getLogger(LegacyExternalPagedStore.class);

    protected byte[] pageEncode(TreePage page) {
        numPagesEncoded.getAndIncrement();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
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
            Bytes.writeLength(page.map.size(), os);

            byte[] firstKeyEncoded = keyCoder.keyEncode(page.getFirstKey());
            byte[] nextFirstKeyEncoded = keyCoder.keyEncode(page.getNextFirstKey());

            updateHistogram(encodeFirstKeySize, firstKeyEncoded.length);
            updateHistogram(encodeNextFirstKeySize, nextFirstKeyEncoded.length);

            Bytes.writeBytes(firstKeyEncoded, os);
            Bytes.writeBytes(nextFirstKeyEncoded, os);


            for (Map.Entry<K, PageValue> e : page.map.entrySet()) {
                byte[] nextKey = keyCoder.keyEncode(e.getKey());
                byte[] nextValue = e.getValue().raw(KeyCoder.EncodeType.LEGACY);

                updateHistogram(encodeKeySize, nextKey.length);
                updateHistogram(encodeValueSize, nextValue.length);

                Bytes.writeBytes(nextKey, os);
                Bytes.writeBytes(nextValue, os);
            }
            Bytes.writeLength((page.estimateTotal > 0 ? page.estimateTotal : 1), os);
            Bytes.writeLength((page.estimates > 0 ? page.estimates : 1), os);
            switch (gztype) {
                case 1:
                    ((DeflaterOutputStream) os).finish();
                    break;
                case 2:
                    ((GZOut) os).finish();
                    break;
            }
            os.flush();
            os.close();
            byte[] bytesOut = out.toByteArray();
            if (log.isDebugEnabled()) log.debug("encoded " + bytesOut.length + " bytes to " + page);
            updateHistogram(encodePageSize, bytesOut.length);
            updateHistogram(numberKeysPerPage, page.map.size());
            return bytesOut;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected TreePage pageDecode(byte[] page) {
        numPagesDecoded.getAndIncrement();
        try {
            InputStream in = new ByteArrayInputStream(page);
            int flags = in.read() & 0xff;
            int gztype = flags & 0x0f;
            boolean hasEstimates = (flags & FLAGS_HAS_ESTIMATES) != 0;
            switch (gztype) {
                case 1:
                    in = new InflaterInputStream(in);
                    break;
                case 2:
                    in = new GZIPInputStream(in);
                    break;
                case 3:
                    in = new LZFInputStream(in);
                    break;
                case 4:
                    in = new SnappyInputStream(in);
                    break;
            }
            int entries = (int) Bytes.readLength(in);
            int count = entries;
            K firstKey = keyCoder.keyDecode(Bytes.readBytes(in));
            K nextFirstKey = keyCoder.keyDecode(Bytes.readBytes(in));
            TreePage decode = new TreePage(firstKey).setNextFirstKey(nextFirstKey);
            int bytes = 0;
            while (count-- > 0) {
                byte kb[] = Bytes.readBytes(in);
                byte vb[] = Bytes.readBytes(in);
                bytes += kb.length + vb.length;
                K key = keyCoder.keyDecode(kb);
                decode.map.put(key, new PageValue(vb, KeyCoder.EncodeType.LEGACY));
            }
            if (maxTotalMem > 0) {
                if (hasEstimates) {
                    decode.setAverage((int) Bytes.readLength(in), (int) Bytes.readLength(in));
                } else {
                    /** use a pessimistic/conservative byte/entry estimate */
                    decode.setAverage(bytes * estimateMissingFactor, entries);
                }
            }
            in.close();
            if (log.isDebugEnabled()) log.debug("decoded " + decode);
            return decode;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
