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

import java.io.UnsupportedEncodingException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.addthis.basis.util.Varint;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.BytesCodable;
import com.addthis.codec.codables.Codable;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;


/**
 * Class that helps maintain a top N list for any String Map.
 */
public final class KeyTopper implements Codable, BytesCodable {

    private static final byte[] EMPTY = new byte[0];

    public KeyTopper() {
    }

    @FieldConfig(codable = true, required = true)
    private HashMap<String, Long> map;
    /**
     * Minimum value in the data structure. Not serialized
     * to byte representation. Regenerated as needed.
     */
    @FieldConfig(codable = true)
    private long minVal;
    /**
     * Minimum key in the data structure. Not serialized
     * to byte representation. Regenerated as needed.
     */
    @FieldConfig(codable = true)
    private String minKey;
    @FieldConfig(codable = true)
    private boolean lossy;

    @Override
    public String toString() {
        return "topper(min:" + minKey + "=" + minVal + "->" + map.toString() + ",lossy:" + lossy + ")";
    }

    public KeyTopper init() {
        map = new HashMap<>();
        return this;
    }

    public KeyTopper setLossy(boolean isLossy) {
        lossy = isLossy;
        return this;
    }

    public boolean isLossy() {
        return lossy;
    }

    public int size() {
        return map.size();
    }

    public Long get(String key) {
        return map.get(key);
    }

    private static final Comparator<Map.Entry<String,Long>> ENTRIES_COMPARATOR =
            (arg0, arg1) -> (int) (arg1.getValue() - arg0.getValue());

    /**
     * returns the list sorted by greatest to least count.
     */
    @SuppressWarnings("unchecked")
    public Map.Entry<String, Long>[] getSortedEntries() {
        Map.Entry<String,Long>[] e = new Map.Entry[map.size()];
        e = map.entrySet().toArray(e);
        Arrays.sort(e, ENTRIES_COMPARATOR);
        return e;
    }

    /**
     * Recreate the minimum key and minimum value if the map
     * contains one or more elements and current minimum key is null
     * or the {@code force} parameter is true. Use {@code force}
     * when the minimum key has been evicted from the data structure
     * or the count associated with the minimum key has been updated.
     *
     * Postcondition: Either the top N is empty or the minimum key
     * is a non-null value.
     *
     * @param force if true then always recreate minimum key and value
     */
    private void recreateMinimum(boolean force) {
        if (map.size() > 0 && (minKey == null || force)) {
            minVal = Long.MAX_VALUE;
            for (Map.Entry<String, Long> e : this.map.entrySet()) {
                if (e.getValue() < minVal) {
                    minVal = e.getValue();
                    minKey = e.getKey();
                }
            }
        }
        assert((minKey != null) ^ (map.size() == 0));
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     *
     * @param id
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(@Nonnull String id, int maxsize) {
        return increment(id, 1, maxsize);
    }

    /**
     * Adds 'ID' the top N if: 1) there are more empty slots or 2) count >
     * smallest top count in the list
     * This one increments weight
     *
     * @param id
     * @param weight
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(@Nonnull String id, int weight, int maxsize) {
        Long count = map.get(id);
        if (count == null) {
            if (lossy && map.size() >= maxsize) {
                recreateMinimum(false);
                count = minVal;
            } else {
                count = 0L;
            }
        }
        return update(id, count + weight, maxsize);
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
    public boolean incrementExisting(@Nonnull String id) {
        Long value = map.get(id);
        if (value != null) {
            map.put(id, value + 1L);
            if (id.equals(minKey)) {
                recreateMinimum(true);
            }
            return true;
        }
        return false;
    }

    /**
     * Adds 'id' the top N if: (1) there are more empty slots or
     * (2) value > minimum value in the top N.
     *
     * @param id       key to insert or update
     * @param value    count to associate with the key
     * @return element dropped from top or null if accepted into top with no
     *         drops. returns the offered key if it was rejected for update
     *         or inclusion in the top.
     */
    public String update(@Nonnull String id, long value, int maxsize) {
        Preconditions.checkArgument(value >= 0, "Argument was %s but expected nonnegative", value);
        Preconditions.checkArgument(maxsize > 0, "Argument was %s but expected positive integer", maxsize);
        String result = null;
        /** compute minimum key and value if they are missing */
        recreateMinimum(false);
        /** insert or update key. Evict if necessary */
        if (value >= minVal) {
            /** only remove if topN is full and we're not updating an existing entry */
            boolean remove = (map.size() >= maxsize) && !map.containsKey(id) && (minKey != null);
            if (remove) {
                map.remove(minKey);
                result = minKey;
            }
            /** update or add entry */
            map.put(id, value);
            /** recalculate min *only* if the min entry was removed or updated */
            if (remove || id.equals(minKey)) {
                recreateMinimum(true);
            }
        }
        /** insert or update key. No eviction necessary. */
        else if (map.size() < maxsize) {
            map.put(id, value);
            if (id.equals(minKey)) {
                recreateMinimum(true);
            } else if (value < minVal) {
                minKey = id;
                minVal = value;
            }
        }
        /** not eligible for top */
        else {
            result = id;
        }
        return result;
    }

    @Override public byte[] bytesEncode(long version) {
        if (map.size() == 0) {
            return EMPTY;
        }
        byte[] retBytes = null;
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            Varint.writeUnsignedVarInt(map.size(), byteBuf);
            for (Map.Entry<String, Long> mapEntry : map.entrySet()) {
                String key = mapEntry.getKey();
                if (key == null) {
                    throw new NullPointerException("KeyTopper decoded null key");
                }
                byte[] keyBytes = key.getBytes("UTF-8");
                Varint.writeUnsignedVarInt(keyBytes.length, byteBuf);
                byteBuf.writeBytes(keyBytes);
                Varint.writeUnsignedVarLong(mapEntry.getValue(), byteBuf);
            }
            retBytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(retBytes);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        } finally {
            byteBuf.release();
        }
        return retBytes;
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        map = new HashMap<>();
        if (b.length == 0) {
            return;
        }
        ByteBuf byteBuf = Unpooled.wrappedBuffer(b);
        try {
            int mapSize = Varint.readUnsignedVarInt(byteBuf);
            try {
                if (mapSize > 0) {
                    for (int i = 0; i < mapSize; i++) {
                        int keyLength = Varint.readUnsignedVarInt(byteBuf);
                        byte[] keybytes = new byte[keyLength];
                        byteBuf.readBytes(keybytes);
                        String k = new String(keybytes, "UTF-8");
                        long value = Varint.readUnsignedVarLong(byteBuf);
                        map.put(k, value);
                    }
                }
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } finally {
            byteBuf.release();
        }
    }

    public long getMinVal() {
        return minVal;
    }

    public String getMinKey() {
        return minKey;
    }
}
