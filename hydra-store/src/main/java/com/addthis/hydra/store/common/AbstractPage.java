package com.addthis.hydra.store.common;

import com.addthis.basis.io.GZOut;
import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Varint;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.hydra.store.kv.KeyCoder;
import com.addthis.hydra.store.kv.PageEncodeType;
import com.addthis.hydra.store.skiplist.LockMode;
import com.addthis.hydra.store.skiplist.SkipListCache;
import com.google.common.base.Throwables;
import com.jcraft.jzlib.Deflater;
import com.jcraft.jzlib.DeflaterOutputStream;
import com.jcraft.jzlib.InflaterInputStream;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;
import com.yammer.metrics.core.Histogram;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPInputStream;

public abstract class AbstractPage<K, V extends BytesCodable> implements Page<K, V> {

    public static final int gzlevel = Parameter.intValue("eps.gz.level", 1);
    public static final int gztype = Parameter.intValue("eps.gz.type", 1);
    public static final int gzbuf = Parameter.intValue("eps.gz.buffer", 1024);
    public static final int estimateMissingFactor = Parameter.intValue("eps.mem.estimate.missing.factor", 8);
    public static final int memEstimationStrategy = Parameter.intValue("eps.mem.estimate.method", 1);
    public static final int estimateRollMin = Parameter.intValue("eps.mem.estimate.roll.min", 1000);
    public static final int estimateRollFactor = Parameter.intValue("eps.mem.estimate.roll.factor", 100);

    protected final AbstractPageCache<K, V> parent;

    public final K firstKey;

    public K nextFirstKey;

    /**
     * This value is updated each time the node is accessed.
     */
    volatile long timeStamp;

    int size;

    @Nullable
    private ArrayList<K> keys;

    @Nullable
    private ArrayList<V> values;

    @Nullable
    private ArrayList<byte[]> rawValues;

    @Nonnull
    private ExternalMode state;

    private int estimateTotal, estimates, avgEntrySize;

    private int memoryEstimate;

    private PageEncodeType encodeType;

    public static final int ESTIMATES_BIT_OFFSET = 4;
    public static final int TYPE_BIT_OFFSET = 5;
    public static final int FLAGS_HAS_ESTIMATES = 1 << ESTIMATES_BIT_OFFSET;

    public final KeyCoder<K, V> keyCoder;

    private final ReentrantReadWriteLock lock;

    /**
     * This value is incremented each time the write lock
     * is released.
     */
    @GuardedBy("lock")
    long writeStamp;

    @Override
    public long getWriteStamp() {
        return writeStamp;
    }

    public void incrementWriteStamp() {
        writeStamp++;
    }

    public AbstractPage(AbstractPageCache<K, V> cache, K firstKey, K nextFirstKey, PageEncodeType encodeType) {
        this.parent = cache;
        this.keyCoder = parent != null ? parent.keyCoder : null;
        this.firstKey = firstKey;
        this.nextFirstKey = nextFirstKey;
        this.timeStamp = AbstractPageCache.generateTimestamp();
        this.state = ExternalMode.DISK_MEMORY_IDENTICAL;
        this.encodeType = encodeType;
        this.lock = initLock();
    }

    public ReentrantReadWriteLock initLock() {
        // default implementation does nothing, subclasses may override
        return null;
    }


    public AbstractPage(AbstractPageCache<K, V> cache, K firstKey,
                        K nextFirstKey, int size, ArrayList<K> keys, ArrayList<V> values,
                        ArrayList<byte[]> rawValues, PageEncodeType encodeType) {
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
        this.timeStamp = AbstractPageCache.generateTimestamp();
        this.state = ExternalMode.DISK_MEMORY_IDENTICAL;
        this.encodeType = encodeType;
        this.lock = initLock();
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
        modeUnlock(LockMode.READMODE);
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

    /**
     * Generate a blank page.
     */
    @Override
    public void initialize() {
        keys = new ArrayList<>();
        values = new ArrayList<>();
        rawValues = new ArrayList<>();
        size = 0;
        timeStamp = AbstractPageCache.generateTimestamp();
    }


    protected final void updateHistogram(Histogram histogram, int value, boolean record) {
        /**
         *  The JIT compiler should be smart enough to eliminate this code
         *  when {@link SkipListCache.trackEncodingByteUsage} is false.
         */
        if (AbstractPageCache.trackEncodingByteUsage && record) {
            histogram.update(value);
        }
    }

    @Override
    public byte[] encode(ByteBufOutputStream out) {
        return encode(out, true);
    }

    public byte[] encode(ByteBufOutputStream out, boolean record) {
        PageCacheMetrics<K, V> metrics = parent.metrics;
        parent.numPagesEncoded.getAndIncrement();
        PageEncodeType upgradeType = PageEncodeType.defaultType();
        try {
            OutputStream os = out;
            out.write(gztype | FLAGS_HAS_ESTIMATES | (upgradeType.ordinal() << TYPE_BIT_OFFSET));
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
                byte[] keyEncoded = keyCoder.keyEncode(keys.get(i), firstKey, upgradeType);
                byte[] rawVal = rawValues.get(i);

                if (rawVal == null || upgradeType != encodeType) {
                    fetchValue(i);
                    rawVal = keyCoder.valueEncode(values.get(i), upgradeType);
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
            throw Throwables.propagate(ex);
        }
    }


    public void decode(byte[] page) {
        parent.numPagesDecoded.getAndIncrement();
        ByteBuf buffer = Unpooled.wrappedBuffer(page);
        try {
            InputStream in = new ByteBufInputStream(buffer);
            int flags = in.read() & 0xff;
            int gztype = flags & 0x0f;
            int pageType = flags >>> TYPE_BIT_OFFSET;
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
            DataInputStream dis = null;
            switch (pageType) {
                case 0:
                    encodeType = PageEncodeType.LEGACY;
                    break;
                case 1:
                    encodeType = PageEncodeType.SPARSE;
                    dis = new DataInputStream(in);
                    break;
                case 2:
                    encodeType = PageEncodeType.LONGIDS;
                    dis = new DataInputStream(in);
                    break;
            }
            decodeKeysAndValues(encodeType, in, dis, hasEstimates);
            in.close();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        } finally {
            buffer.release();
        }
    }

    /**
     * @param encodeType
     * @param in
     * @param dis
     * @param hasEstimates
     * @throws IOException
     */
    private void decodeKeysAndValues(PageEncodeType encodeType, InputStream in,
                                     DataInputStream dis, boolean hasEstimates) throws IOException {
        K firstKey;
        byte[] nextFirstKeyBytes;
        int readEstimateTotal;
        int readEstimates;
        int entries = encodeType.readInt(in, dis);

        firstKey = keyCoder.keyDecode(encodeType.readBytes(in, dis));
        nextFirstKeyBytes = encodeType.nextFirstKey(in, dis);
        nextFirstKey = keyCoder.keyDecode(nextFirstKeyBytes);
        assert (this.firstKey.equals(firstKey));

        int bytes = 0;

        size = entries;
        keys = new ArrayList<>(size);
        values = new ArrayList<>(size);
        rawValues = new ArrayList<>(size);

        for (int i = 0; i < entries; i++) {
            byte[] kb = encodeType.readBytes(in, dis);
            byte[] vb = encodeType.readBytes(in, dis);
            bytes += kb.length + vb.length;
            keys.add(keyCoder.keyDecode(kb, firstKey, encodeType));
            values.add(null);
            rawValues.add(vb);
        }

        if (hasEstimates) {
            readEstimateTotal = encodeType.readInt(in, dis);
            readEstimates = encodeType.readInt(in, dis);
            setAverage(readEstimateTotal, readEstimates);
        } else {
            /** use a pessimistic/conservative byte/entry estimate */
            setAverage(bytes * estimateMissingFactor, entries);
        }

        updateMemoryEstimate();
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

    public void updateAverage(K key, V val, int count) {
        long next = parent.estimateCounter.incrementAndGet();
        if (avgEntrySize == 0 ||
                (parent.getEstimateInterval() <= 0 && estimates > 0 && next % estimates == 0) ||
                (parent.getEstimateInterval() > 0 && next % parent.getEstimateInterval() == 0)) {
            switch (memEstimationStrategy) {
                case 0:
                    /** use encoded byte size as crude proxy for mem size */
                    updateAverage((keyCoder.keyEncode(key).length +
                            keyCoder.valueEncode(val, encodeType).length), count);
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
        } else if (parent.getMaxPageMem() > 0 && estimatedMem() > parent.getMaxPageMem()) {
            return true;
        } else if (parent.getMaxPageSize() > 0) {
            if (size > parent.getMaxPageSize()) {
                return true;
            }
        } else if (size > AbstractPageCache.defaultMaxPageEntries) {
            return true;
        }
        return false;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public K getNextFirstKey() {
        return nextFirstKey;
    }

    @Override
    public void setNextFirstKey(K nextFirstKey) {
        this.nextFirstKey = nextFirstKey;
    }

    @Override
    public int getAvgEntrySize() {
        return avgEntrySize;
    }

    @Override
    public void setAvgEntrySize(int avgEntrySize) {
        this.avgEntrySize = avgEntrySize;
    }

    @Override
    public int getEstimates() {
        return estimates;
    }

    @Override
    public void setEstimates(int estimates) {
        this.estimates = estimates;
    }

    @Override
    public int getEstimateTotal() {
        return estimateTotal;
    }

    @Override
    public void setEstimateTotal(int estimateTotal) {
        this.estimateTotal = estimateTotal;
    }

    @Override
    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public void setKeys(ArrayList<K> keys) {
        this.keys = keys;
    }

    @Override
    public void setValues(ArrayList<V> values) {
        this.values = values;
    }

    @Override
    public void setRawValues(ArrayList<byte[]> rawValues) {
        this.rawValues = rawValues;
    }

    @Override
    public long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public boolean inTransientState() {
        return state.isTransient();
    }

    @Override
    public PageEncodeType getEncodeType() {
        return encodeType;
    }

    @Override
    public ExternalMode getState() {
        return state;
    }

    @Override
    public void setState(ExternalMode state) {
        this.state = state;
    }

    @Override
    public ArrayList<K> keys() {
        return keys;
    }

    @Override
    public ArrayList<V> values() {
        return values;
    }

    @Override
    public ArrayList<byte[]> rawValues() {
        return rawValues;
    }

    @Override
    public K getFirstKey() {
        return firstKey;
    }
}
