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

import java.io.UnsupportedEncodingException;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.addthis.basis.util.Varint;

import com.addthis.codec.Codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

public final class KeyTopper implements Codec.Codable, Codec.SuperCodable,
        Codec.BytesCodable {

    private static final byte[] EMPTY = new byte[0];

    private static final Logger log = LoggerFactory.getLogger(KeyTopper.class);

    /**
     * Construct a top-K counter with a size of zero.
     * The key topper will allocate memory upon the first insert operation.
     */
    public KeyTopper() {
        values = new long[0];
        locations = new HashMap<>();
        keys = new String[0];
        size = 0;
    }

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private HashMap<String, Long> map;

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private long minVal;

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private String minKey;

    /**
     * Is not used during the lifetime of the object.
     * Used for backwards compatibility when decoding an object.
     */
    @Codec.Set(codable = true)
    private boolean lossy;

    /**
     * These fields are used by the class. Any other
     * fields are for backwards compatibility.
     * keys are stored in unsorted order.
     * values are stored in unsorted order.
     * locations assign strings to a position in the two arrays.
     * size represents the number of items currently in the object.
     */
    private Map<String,Integer> locations;
    private String[] keys;
    private long[] values;
    private int size;

    @Override
    public String toString() {
        return "topper(keys: " +
               Arrays.toString(keys) +
               " values: " +
               Arrays.toString(values) + ")";
    }

    private static class StringLong implements Comparable<StringLong> {

        private final String key;
        private final long value;

        StringLong(String key, long value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(StringLong other) {
            return Long.compare(this.value, other.value);
        }
    }

    /**
     * returns the list sorted by greatest to least count.
     * This is an expensive operation.
     */
    public Map.Entry<String, Long>[] getSortedEntries() {

        Map.Entry<String, Long> e[] = new Map.Entry[size];
        StringLong sl[] = new StringLong[size];
        for(int i = 0; i < size; i++) {
            sl[i] = new StringLong(keys[i], values[i]);
        }
        Arrays.sort(sl);
        for(int i = 0; i < size; i++) {
            e[i] = new AbstractMap.SimpleImmutableEntry<>(sl[i].key, sl[i].value);
        }
        return e;
    }

    /**
     * Resize the collections as requested.
     *
     * @param capacity
     */
    private void resize(int capacity) {
        if (capacity > values.length) {
            long[] newValues = new long[capacity];
            String[] newNames = new String[capacity];
            System.arraycopy(values, 0, newValues, 0, size);
            System.arraycopy(keys, 0, newNames, 0, size);
            values = newValues;
            keys = newNames;
        } else if (capacity < values.length) {
            Map.Entry<String,Long>[] sorted = getSortedEntries();
            Map<String,Integer> newLocations = new HashMap<>();
            long[] newValues = new long[capacity];
            String[] newKeys = new String[capacity];
            int newSize = Math.min(size, capacity);
            for(int i = 0; i < newSize; i++) {
                Map.Entry<String,Long> entry = sorted[i];
                newKeys[i] = entry.getKey();
                newValues[i] = entry.getValue();
                newLocations.put(entry.getKey(), i);
            }
            keys = newKeys;
            values = newValues;
            locations = newLocations;
            size = newSize;
        }
    }

    /**
     * Scan through all elements to find the minimum element.
     */
    private int selectMinElement() {
        long minValue = Long.MAX_VALUE;
        int position = -1;
        for(int i = 0; i < size; i++) {
            if (values[i] < minValue) {
                minValue = values[i];
                position = i;
            }
        }
        return position;
    }

    /**
     * Inserts the key {@code key} with a corresponding weight.
     * This will evict another key with the current minimum weight.
     * If {@code additive} is true then the new value associated
     * with {@code key} is the current minimum + weight. Otherwise
     * the new value is the weight. The correct implementation of
     * the top-K data structure assumes that {@code additive} is true.
     *
     * @param id        new key to insert
     * @param weight    weight associated with key
     * @param position  position of the minimum element
     * @param additive  see comment above
     * @return          old key that is removed
     */
    private String replace(String id, long weight, int position, boolean additive) {
        String evicted = keys[position];
        locations.remove(evicted);
        locations.put(id, position);
        keys[position] = id;
        values[position] = weight + (additive ? values[position] : 0);
        return evicted;
    }

    /**
     * Updates the key {@code id} using a weight of 1.
     * If the key is already in the top-K then increment its weight.
     * Otherwise evict a key with the minimum value and replace
     * it with (key, minimum + 1).
     *
     * @param id
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(String id, int maxsize) {
        return increment(id, 1, maxsize);
    }

    /**
     * Updates the key {@code id} with the specified weight.
     * If the key is already in the top-K then update the weight.
     * Otherwise evict a key with the minimum value and replace
     * it with (key, minimum + weight).
     *
     * @param id
     * @param weight
     * @return element dropped from top or null if accepted into top with no
     *         drops
     */
    public String increment(String id, int weight, int maxsize) {
        Integer position = locations.get(id);
        if (position != null) {
            values[position] += weight;
            return null;
        }
        resize(maxsize);
        if (size < values.length) {
            values[size] = weight;
            keys[size] = id;
            locations.put(id, size);
            size++;
            return null;
        }
        return replace(id, weight, selectMinElement(), true);
    }

    /**
     * Increments the count for 'ID' in the top map if 'ID' already exists in
     * the map.
     *
     * @param id the id to increment if it already exists in the map
     * @return whether the element was in the map
     */
    public boolean incrementExisting(String id) {
        Integer position = locations.get(id);
        if (position != null) {
            values[position]++;
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * This method is maintained for legacy purposes although it does not
     * conform to the standard behavior of a top-K data structure.
     * It adds {@code id} the data structure if (1) there are more empty slots
     * or (2) count > smallest top count in the list. A correct top-K update
     * operation should always insert the new key into the data structure.
     * Use {@link #increment(String, int)} or {@link #increment(String, int, int)}
     * to maintain the correct semantics for top-K items.
     *
     * @param id
     * @param count
     * @return element dropped from top or null if accepted into top with no
     *         drops. returns the offered key if it was rejected for update
     *         or inclusion in the top.
     **/
    public String update(String id, long count, int maxsize) {
        Integer position = locations.get(id);
        if (position != null) {
            values[position] = count;
            return null;
        }
        resize(maxsize);
        if (size < values.length) {
            values[size] = count;
            locations.put(id, size);
            keys[size] = id;
            size++;
            return null;
        } else {
            int minPosition = selectMinElement();
            if (count <= values[minPosition]) {
                return id;
            } else {
                return replace(id, count, minPosition, false);
            }
        }
    }

    public int size() {
        return size;
    }


    /**
     * Returns either the estimated count associated with the key,
     * or null if the key is not present in the top-K items.
     *
     * @param key
     * @return
     */
    public Long get(String key) {
        Integer position = locations.get(key);
        if (position != null) {
            return values[position];
        } else {
            return null;
        }
    }

    @Override
    public byte[] bytesEncode(long version) {

        if (size == 0) {
            return EMPTY;
        }
        String key = null;
        long value;
        byte[] retBytes = null;
        ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        try {
            Varint.writeUnsignedVarInt(size, byteBuf);
            for (int index = 0; index < size; index++) {
                key = keys[index];
                value = values[index];
                if (key == null) {
                    throw new IllegalStateException("KeyTopper decoded null key");
                }
                byte[] keyBytes = key.getBytes("UTF-8");
                Varint.writeUnsignedVarInt(keyBytes.length, byteBuf);
                byteBuf.writeBytes(keyBytes);
                Varint.writeUnsignedVarLong(value, byteBuf);
            }
            retBytes = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(retBytes);
        } catch (UnsupportedEncodingException e) {
            log.error("Unexpected error while encoding \"" + key + "\"", e);
            throw new RuntimeException(e);
        } finally {
            byteBuf.release();
        }
        return retBytes;
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        if (b.length == 0) {
            return;
        }
        ByteBuf byteBuf = Unpooled.wrappedBuffer(b);
        byte[] keybytes = null;
        try {
            int newsize = Varint.readUnsignedVarInt(byteBuf);
            resize(newsize);
            size = newsize;
            for (int index = 0; index < size; index++) {
                int keyLength = Varint.readUnsignedVarInt(byteBuf);
                keybytes = new byte[keyLength];
                byteBuf.readBytes(keybytes);
                String key = new String(keybytes, "UTF-8");
                long value = Varint.readUnsignedVarLong(byteBuf);
                keys[index] = key;
                values[index] = value;
                locations.put(key, index);
            }
        } catch (UnsupportedEncodingException e) {
            log.error("Unexpected error while decoding \"" +
                      Arrays.toString(keybytes) + "\"", e);
            throw new RuntimeException(e);
        } finally {
            byteBuf.release();
        }
    }

    /**
     * The {@link #bytesEncode(long)} method does not need any
     * behavior from this method.
     */
    @Override
    public void preEncode() {}

    /**
     * This method is responsible for converting the old legacy fields
     * {@link #map}, {@link #minVal} and {@link #minKey} into the new
     * fields.
     **/
    @Override
    public void postDecode() {
        int newsize = map.size();
        resize(newsize);
        size = newsize;
        int index = 0;
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            keys[index] = entry.getKey();
            values[index] = entry.getValue();
            locations.put(entry.getKey(), index);
            index++;
        }
        map = null;
    }
}
