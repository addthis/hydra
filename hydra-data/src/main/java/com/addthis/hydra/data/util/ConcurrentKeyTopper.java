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
package com.addthis.hydra.data.util;

import javax.annotation.Nonnull;

import java.io.DataInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.MemoryCounter;

import com.addthis.codec.Codec;

import com.addthis.hydra.store.util.Varint;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import jsr166e.ConcurrentHashMapV8;

/**
 * Class that helps maintain a top N list for any String Map TODO should move
 * into basis libraries
 */
public final class ConcurrentKeyTopper implements Codec.SuperCodable, Codec.BytesCodable {

    private static final byte[] EMPTY = new byte[0];

    public ConcurrentKeyTopper() {
    }

    @Codec.Set(codable = true, required = true)
//  @MemoryCounter.Mem(estimate = false, size = 50000)
    private ConcurrentHashMapV8<String, Long> map;

    @SuppressWarnings("unused")
    @Codec.Set(codable = true)
    private long minVal;

    @SuppressWarnings("unused")
    @Codec.Set(codable = true)
    private String minKey;

    @Codec.Set(codable = true)
    private boolean lossy;

    // If maxSizeLimit is true then map.size() >= maxSize
    private volatile boolean maxSizeLimit = false;

    @MemoryCounter.Mem(estimate = false, size = 64)
    private final AtomicReference<String> staging = new AtomicReference<>();

    @MemoryCounter.Mem(estimate = false, size = 64)
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public String toString() {
        return "topper(map:" + map.toString() + ",lossy:" + lossy + ")";
    }

    public ConcurrentKeyTopper init(int size) {
        map = new ConcurrentHashMapV8<>(size + 16, 0.75f, 4);
        this.lossy = true;

        return this;
    }

    public ConcurrentKeyTopper init() {
        map = new ConcurrentHashMapV8<>(16, 0.75f, 4);
        this.lossy = true;
        return this;
    }

    @SuppressWarnings("unused")
    public boolean isLossy() {
        return lossy;
    }

    public int size() {
        return map.size();
    }

    public Long get(String key) {
        return map.get(key);
    }

    private String findMinKey() {
        String minKey = null;
        long minValue = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            long value = entry.getValue();
            if (value < minValue) {
                minValue = value;
                minKey = entry.getKey();
            }
        }

        return minKey;
    }

    /**
     * returns the list sorted by greatest to least count.
     */
    @SuppressWarnings("unchecked")
    public Map.Entry<String, Long>[] getSortedEntries() {
        lock.readLock().lock();

        try {
            int size = map.size();
            Map.Entry e[] = new Map.Entry[size];
            e = map.entrySet().toArray(e);
            Arrays.sort(e, new Comparator() {
                public int compare(Object arg0, Object arg1) {
                    return Long.compare(
                            ((Map.Entry<String, Long>) arg1).getValue(),
                            ((Map.Entry<String, Long>) arg0).getValue());
                }
            });
            return e;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Precondition: the readlock is held
     * Postcondition: the readlock is held
     */
    private String evictAnotherKey(@Nonnull String newKey, int weight) {
        assert (weight > 0);

        lock.readLock().unlock();
        boolean writeLock = false;

        try {
            lock.writeLock().lock();
            writeLock = true;

            if (map.containsKey(newKey)) {
                return null;
            }

            // the staging area is only used when weight is one
            if (weight == 1) {
                String currentStage = staging.get();

                if (!newKey.equals(currentStage)) {
                    return null;
                }
            }

            String key = findMinKey();

            long minValue = map.get(key);

            map.remove(key);

            // if weight is one then we used the staging area
            // and that should be counted as an instance
            if (weight == 1) {
                map.put(newKey, minValue + 1);
                staging.set(null);
            } else {
                map.put(newKey, minValue + weight - 1);
            }

            return key;
        } finally {
            lock.readLock().lock();

            if (writeLock) {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     *
     * @return element dropped from top or null if accepted into top with no
     * drops
     */
    public String increment(@Nonnull String id, int maxSize) {
        return increment(id, 1, maxSize);
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     * This one increments weight
     *
     * @return element dropped from top or null if accepted into top with no
     * drops
     */
    public String increment(@Nonnull String id, int weight, int maxSize) {
        assert (weight > 0);

        lock.readLock().lock();

        try {
            while (true) {
                Long currentValue = map.get(id);
                if (currentValue == null) {
                    if (!maxSizeLimit && map.size() < maxSize) {
                        if (map.putIfAbsent(id, (long) weight) == null) {
                            return null;
                        }
                    } else {
                        maxSizeLimit = true;
                        if (weight == 1) {
                            String currentStage = staging.get();
                            if (!id.equals(currentStage)) {
                                if (staging.compareAndSet(currentStage, id)) {
                                    return currentStage;
                                } else {
                                    continue;
                                }
                            }
                        }
                        String evicted = evictAnotherKey(id, weight);
                        if (evicted != null) {
                            return evicted;
                        }
                    }
                } else if (map.replace(id, currentValue, currentValue + weight)) {
                    return null;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Increments the count for 'ID' in the top map if 'ID' already exists in
     * the map. This method is used if you want to increment a lossy top without
     * removing an element. Used when there is a two stage update for new data
     * elements
     *
     * @param id the id to increment if it already exists in the map
     * @return whether the element was in the map
     */
    @SuppressWarnings("unused")
    public boolean incrementExisting(String id) {
        lock.readLock().lock();

        try {
            while (true) {
                Long currentValue = map.get(id);
                if (currentValue == null) {
                    return false;
                } else if (map.replace(id, currentValue, currentValue + 1)) {
                    return true;
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void postDecode() {
    }

    @Override
    public void preEncode() {
        String key = findMinKey();

        if (key != null) {
            minKey = key;
            minVal = map.get(key);
        }
    }

    @Override
    public byte[] bytesEncode() {
        preEncode();
        if (minKey == null || minKey.isEmpty()) {
            return EMPTY;
        }
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();

        if (minKey != null) {
            try {
                byte[] minKeyBytes = minKey.getBytes("UTF-8");
                int minKeyLength = minKeyBytes.length;
                Varint.writeUnsignedVarInt(minKeyLength, byteBuf);
                byteBuf.writeBytes(minKeyBytes);
                Varint.writeUnsignedVarLong(minVal, byteBuf);
                byteBuf.writeBoolean(lossy);
                Varint.writeUnsignedVarInt(map.size(), byteBuf);
                if (map.size() > 0) {
                    for (Map.Entry<String, Long> mapEntry : map.entrySet()) {
                        byte[] keyBytes = mapEntry.getKey().getBytes("UTF-8");
                        Varint.writeUnsignedVarInt(keyBytes.length, byteBuf);
                        byteBuf.writeBytes(keyBytes);
                        Varint.writeUnsignedVarLong(mapEntry.getValue(), byteBuf);
                    }
                }
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        byte[] retBytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(retBytes);
        byteBuf.release();
        return retBytes;

    }

    @Override
    public void bytesDecode(byte[] b) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(b);
        try {
            int minKeyLength = Varint.readUnsignedVarInt(byteBuf);
            byte[] minkKeyBytes = new byte[minKeyLength];
            byteBuf.readBytes(minkKeyBytes);
            minKey = new String(minkKeyBytes, "UTF-8");
            minVal = Varint.readUnsignedVarLong(byteBuf);
            lossy = byteBuf.readBoolean();
            int mapSize = Varint.readUnsignedVarInt(byteBuf);

            if (mapSize > 0) {
                map = new ConcurrentHashMapV8<>(mapSize + 16, 0.75f, 4);
                for (int i = 0; i < mapSize; i++) {
                    int keyLength = Varint.readUnsignedVarInt(byteBuf);
                    if (keyLength > 0) {
                        byte[] keybytes = new byte[keyLength];
                        byteBuf.readBytes(keybytes);
                        String k = new String(keybytes, "UTF-8");
                        long value = Varint.readUnsignedVarLong(byteBuf);
                        map.put(k, value);
                    }
                }
            } else {
                map = new ConcurrentHashMapV8<>(16, 0.75f, 4);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        postDecode();

    }
}
