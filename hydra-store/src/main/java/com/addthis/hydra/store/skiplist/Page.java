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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

import com.addthis.basis.io.GZOut;
import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Varint;

import com.addthis.codec.Codec; import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.KeyCoder;

import com.jcraft.jzlib.Deflater;
import com.jcraft.jzlib.DeflaterOutputStream;
import com.jcraft.jzlib.InflaterInputStream;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import com.yammer.metrics.core.Histogram;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

public class Page<K, V extends BytesCodable> {

    static final int gzlevel = Parameter.intValue("eps.gz.level", 1);
    static final int gztype = Parameter.intValue("eps.gz.type", 1);
    static final int gzbuf = Parameter.intValue("eps.gz.buffer", 1024);
    static final int estimateMissingFactor = Parameter.intValue("eps.mem.estimate.missing.factor", 8);
    static final int memEstimationStrategy = Parameter.intValue("eps.mem.estimate.method", 1);
    static final int estimateRollMin = Parameter.intValue("eps.mem.estimate.roll.min", 1000);
    static final int estimateRollFactor = Parameter.intValue("eps.mem.estimate.roll.factor", 100);

    final SkipListCache<K, V> parent;

    final K firstKey;

    @GuardedBy("lock")
    K nextFirstKey;

    @Nonnull
    private final ReentrantReadWriteLock lock;

    /**
     * This value is incremented each time the write lock
     * is released.
     */
    @GuardedBy("lock")
    long writeStamp;

    /**
     * This value is updated each time the node is accessed.
     */
    volatile long timeStamp;

    @GuardedBy("lock")
    int size;

    @GuardedBy("lock")
    @Nullable
    ArrayList<K> keys;

    @GuardedBy("lock")
    @Nullable
    ArrayList<V> values;

    @GuardedBy("lock")
    @Nullable
    ArrayList<byte[]> rawValues;

    @GuardedBy("lock")
    @Nonnull
    ExternalMode state;

    @GuardedBy("lock")
    int estimateTotal, estimates, avgEntrySize;

    @GuardedBy("lock")
    private int memoryEstimate;

    @GuardedBy("lock")
    private KeyCoder.EncodeType encodeType;

    protected static final int FLAGS_IS_SPARSE = 1 << 5;
    protected static final int FLAGS_HAS_ESTIMATES = 1 << 4;

    protected final KeyCoder<K, V> keyCoder;

    protected Page(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, KeyCoder.EncodeType encodeType) {
        this.parent = cache;
        this.keyCoder = parent != null ? parent.keyCoder : null;
        this.firstKey = firstKey;
        this.nextFirstKey = nextFirstKey;
        this.lock = new ReentrantReadWriteLock();
        this.timeStamp = SkipListCache.generateTimestamp();
        this.state = ExternalMode.DISK_MEMORY_IDENTICAL;
        this.encodeType = encodeType;
    }

    protected Page(SkipListCache<K, V> cache, K firstKey,
            K nextFirstKey, int size, ArrayList<K> keys, ArrayList<V> values,
            ArrayList<byte[]> rawValues, KeyCoder.EncodeType encodeType) {
        assert (keys != null);
        assert (values != null);
        assert (rawValues != null);

        assert (keys.size() == size);
        assert (values.size() == size);
        assert (rawValues.size() == size);

        this.parent = cache;
        this.keyCoder = parent.keyCoder;
        this.firstKey = firstKey;
        this.nextFirstKey = nextFirstKey;
        this.size = size;
        this.keys = keys;
        this.values = values;
        this.rawValues = rawValues;
        this.lock = new ReentrantReadWriteLock();
        this.timeStamp = SkipListCache.generateTimestamp();
        this.state = ExternalMode.DISK_MEMORY_IDENTICAL;
        this.encodeType = encodeType;
    }

    /**
     * Generate a blank page.
     */
    public void initialize() {
        keys = new ArrayList<>();
        values = new ArrayList<>();
        rawValues = new ArrayList<>();
        size = 0;
        timeStamp = SkipListCache.generateTimestamp();
    }

    protected final void updateHistogram(Histogram histogram, int value, boolean record) {
        /**
         *  The JIT compiler should be smart enough to eliminate this code
         *  when {@link SkipListCache.trackEncodingByteUsage} is false.
         */
        if (SkipListCache.trackEncodingByteUsage && record) {
            histogram.update(value);
        }
    }

    public byte[] encode(ByteBufOutputStream out) {
        return encode(out, true);
    }

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
            byte[] firstKeyEncoded = keyCoder.keyEncode(firstKey);
            byte[] nextFirstKeyEncoded = keyCoder.keyEncode(nextFirstKey);

            updateHistogram(metrics.encodeNextFirstKeySize, nextFirstKeyEncoded.length, record);

            Varint.writeUnsignedVarInt(size, dos);
            Varint.writeUnsignedVarInt(firstKeyEncoded.length, dos);
            dos.write(firstKeyEncoded);
            Varint.writeUnsignedVarInt(nextFirstKeyEncoded.length, dos);
            if (nextFirstKeyEncoded.length > 0) {
                dos.write(nextFirstKeyEncoded);
            }
            for (int i = 0; i < size; i++) {
                byte[] keyEncoded = keyCoder.keyEncode(keys.get(i));
                byte[] rawVal = rawValues.get(i);

                if (rawVal == null || encodeType != KeyCoder.EncodeType.SPARSE) {
                    fetchValue(i);
                    rawVal = keyCoder.valueEncode(values.get(i), KeyCoder.EncodeType.SPARSE);
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
                case 4:
                    os.flush();
                    break;
            }
            os.flush();
            os.close();
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


    public void  decode(byte[] page) {
        parent.numPagesDecoded.getAndIncrement();
        ByteBuf buffer = Unpooled.wrappedBuffer(page);
        try {
            InputStream in = new ByteBufInputStream(buffer);
            int flags = in.read() & 0xff;
            int gztype = flags & 0x0f;
            boolean isSparse = (flags & FLAGS_IS_SPARSE) != 0;
            boolean hasEstimates = (flags & FLAGS_HAS_ESTIMATES) != 0;
            int readEstimateTotal, readEstimates;
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
            K firstKey;
            byte[] nextFirstKey;
            if (isSparse) {
                encodeType = KeyCoder.EncodeType.SPARSE;
                DataInputStream dis = new DataInputStream(in);
                int entries = Varint.readUnsignedVarInt(dis);

                firstKey = keyCoder.keyDecode(Bytes.readBytes(in, Varint.readUnsignedVarInt(dis)));
                int nextFirstKeyLength = Varint.readUnsignedVarInt(dis);
                if (nextFirstKeyLength > 0) {
                    nextFirstKey = Bytes.readBytes(in, nextFirstKeyLength);
                } else {
                    nextFirstKey = null;
                }

                int bytes = 0;

                size = entries;
                keys = new ArrayList<>(size);
                values = new ArrayList<>(size);
                rawValues = new ArrayList<>(size);

                for (int i = 0; i < entries; i++) {
                    byte kb[] = Bytes.readBytes(in, Varint.readUnsignedVarInt(dis));
                    byte vb[] = Bytes.readBytes(in, Varint.readUnsignedVarInt(dis));
                    bytes += kb.length + vb.length;
                    keys.add(keyCoder.keyDecode(kb));
                    values.add(null);
                    rawValues.add(vb);
                }

                if (hasEstimates) {
                    readEstimateTotal = Varint.readUnsignedVarInt(dis);
                    readEstimates = Varint.readUnsignedVarInt(dis);
                    setAverage(readEstimateTotal, readEstimates);
                } else {
                    /** use a pessimistic/conservative byte/entry estimate */
                    setAverage(bytes * estimateMissingFactor, entries);
                }
            } else {
                encodeType = KeyCoder.EncodeType.LEGACY;
                int entries = (int) Bytes.readLength(in);

                firstKey = keyCoder.keyDecode(Bytes.readBytes(in));
                nextFirstKey = Bytes.readBytes(in);

                int bytes = 0;

                size = entries;
                keys = new ArrayList<>(size);
                values = new ArrayList<>(size);
                rawValues = new ArrayList<>(size);

                for (int i = 0; i < entries; i++) {
                    byte kb[] = Bytes.readBytes(in);
                    byte vb[] = Bytes.readBytes(in);
                    bytes += kb.length + vb.length;
                    keys.add(keyCoder.keyDecode(kb));
                    values.add(null);
                    rawValues.add(vb);
                }

                if (hasEstimates) {
                    readEstimateTotal = (int) Bytes.readLength(in);
                    readEstimates = (int) Bytes.readLength(in);
                    setAverage(readEstimateTotal, readEstimates);
                } else {
                    /** use a pessimistic/conservative byte/entry estimate */
                    setAverage(bytes * estimateMissingFactor, entries);
                }
            }

            updateMemoryEstimate();

            assert (this.firstKey.equals(firstKey));

            this.nextFirstKey = keyCoder.keyDecode(nextFirstKey);

            in.close();
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            buffer.release();
        }
    }

    private int estimatedMem() {
        /**
         * We want to account for the three pointers that point
         * the key, the value, and the raw value. The 64-bit JVM
         * should have 8-byte pointers but the HotSpot JVM uses
         * compressed pointers so the true value is somewhere between
         * 4 and 8. Use 4 bytes as an approximation.
         * (3 pointers * 4 bytes) = 12 bytes.
         */

        int weightedAvg = avgEntrySize + 12;

        return (weightedAvg * size);
    }

    void updateAverage(K key, V val, int count) {
        long next = parent.estimateCounter.incrementAndGet();
        if (avgEntrySize == 0 ||
            (parent.estimateInterval <= 0 && estimates > 0 && next % estimates == 0) ||
            (parent.estimateInterval > 0 && next % parent.estimateInterval == 0)) {
            switch (memEstimationStrategy) {
                case 0:
                    /** use encoded byte size as crude proxy for mem size */
                    updateAverage((keyCoder.keyEncode(key).length + keyCoder.valueEncode(val, encodeType).length), count);
                    break;
                case 1:
                    /** walk objects and estimate.  possibly slower and not demonstrably more accurate */
                    updateAverage((int) (MemoryCounter.estimateSize(key) + MemoryCounter.estimateSize(val)), count);
                    break;
                default:
                    throw new IllegalStateException("invalid sample strategy: " + memEstimationStrategy);
            }
        }
    }

    private void updateAverage(int byteCount, int count) {
        assert (byteCount > 0);

        int byteTotal = byteCount * count;
        if (estimates > Math.min(estimateRollMin, size * estimateRollFactor)) {
            estimates = 1;
            estimateTotal = avgEntrySize;
        } else {
            estimates += count;
            estimateTotal += byteTotal;
            avgEntrySize = estimateTotal / estimates;
        }
    }


    private void setAverage(int total, int count) {
        if ((count == 0) || (total == 1 && count == 1)) {
            avgEntrySize = 0;
            estimates = 0;
            estimateTotal = 0;
        } else {
            avgEntrySize = total / count;
            estimates = count;
            estimateTotal = total;
        }
    }

    public int getMemoryEstimate() {
        return memoryEstimate;
    }

    public void updateMemoryEstimate() {
        memoryEstimate = estimatedMem();
    }

    @SuppressWarnings("unused")
    public boolean upgradeLockAndTestStamp(long oldStamp) {
        readUnlock();
        writeLock();
        return (writeStamp == oldStamp);
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    public void writeLock() {
        lock.writeLock().lock();
    }

    public boolean writeTryLock() {
        return lock.writeLock().tryLock();
    }

    public void writeUnlock() {
        writeStamp++;
        lock.writeLock().unlock();
    }

    public void modeLock(LockMode mode) {
        switch (mode) {
            case READMODE:
                lock.readLock().lock();
                break;
            case WRITEMODE:
                lock.writeLock().lock();
                break;
        }
    }

    public void modeUnlock(LockMode mode) {
        switch (mode) {
            case READMODE:
                lock.readLock().unlock();
                break;
            case WRITEMODE:
                writeUnlock();
                break;
        }
    }

    public void downgradeLock() {
        assert (lock.isWriteLockedByCurrentThread());
        readLock();
        writeUnlock();
    }

    public boolean isWriteLockedByCurrentThread() {
        return lock.isWriteLockedByCurrentThread();
    }


    public boolean interval(Comparable<? super K> ckey) {
        assert (ckey.compareTo(firstKey) >= 0);

        if (nextFirstKey == null) {
            return true;
        } else {
            if (ckey.compareTo(nextFirstKey) < 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Given a integer position if {@link #values} is storing a null entry
     * and {@link #rawValues} is storing the representation of a
     * non-null entry then populate {@link #values} with the decoded
     * result of {@link #rawValues}.
     */
    public void fetchValue(int position) {
       V value = values.get(position);
        byte[] rawValue = rawValues.get(position);
        if (value == null) {
            values.set(position, keyCoder.valueDecode(rawValue, encodeType));
        }
    }

    public boolean splitCondition() {
        if (size == 1) {
            return false;
        } else if (parent.maxPageMem > 0 && estimatedMem() > parent.maxPageMem) {
            return true;
        } else if (parent.maxPageSize > 0) {
            if (size > parent.maxPageSize) {
                return true;
            }
        } else if (size > SkipListCache.defaultMaxPageEntries) {
            return true;
        }
        return false;
    }

    public boolean inTransientState() {
        return state.isTransient();
    }

    public KeyCoder.EncodeType getEncodeType() {
        return encodeType;
    }

    public static class DefaultPageFactory<K, V extends BytesCodable> extends PageFactory<K,V> {

        public static final DefaultPageFactory singleton = new DefaultPageFactory();

        private DefaultPageFactory() {}

        @Override
        public Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, KeyCoder.EncodeType encodeType) {
            return new Page(cache, firstKey, nextFirstKey, encodeType);
        }

        @Override
        public Page newPage(SkipListCache<K, V> cache, K firstKey, K nextFirstKey, int size, ArrayList<K> keys,
                ArrayList<V> values, ArrayList<byte[]> rawValues, KeyCoder.EncodeType encodeType) {
            return new Page(cache, firstKey, nextFirstKey, size, keys, values, rawValues, encodeType);
        }
    }
}

