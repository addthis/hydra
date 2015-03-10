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
package com.addthis.hydra.store.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.addthis.basis.util.LessBytes;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.hydra.common.hash.PluggableHashFunction;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * A simple, codable Bloom Filter adhering to the SeenFilter interface.
 * <p>A Bloom filter is a space-efficient probabilistic data structure that is used
 * to test whether an element is a member of a set. False positive matches are
 * possible, but false negatives are not; i.e. a query returns either
 * "inside set (may be wrong)" or "definitely not in set". Elements can be added to
 * the set, but not removed. The more elements that are added to the set,
 * the larger the probability of false positives.
 *
 * @user-reference
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SeenFilterBasic<K> implements SeenFilter<K>, SuperCodable {

    public static final int HASH_HASHCODE = 0; /* mostly bad */
    public static final int HASH_HASHCODE_SHIFT_REV = 1; /* mostly bad */
    public static final int HASH_HASHCODE_LONG_REV = 2; /* mostly bad */
    public static final int HASH_MD5 = 3; /* marginally better accuracy, much slower */
    public static final int HASH_PLUGGABLE_SHIFT = 4; /* default, best blend if speed and accuracy */

    /**
     * for one of the hash types
     */
    private static MessageDigest md5;

    static {
        try {
            md5 = java.security.MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    /**
     * Optionally specify the initial state of the bloom filter.
     * If this field is specified then the {@link #bitsfree} field
     * must also be specified.
     */
    @FieldConfig(codable = true)
    private int[] bitset;

    /**
     * Cardinality of the bloom filter
     * (total number of bits allocated to the filter).
     * This field must be 32 or greater. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private int bits;

    /**
     * Number of hash function evaluations for each insertion
     * operation. This parameter is usually referred to as
     * the "k" parameter in the literature. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private int bitsper;

    /**
     * Type of hash function that is used. The following types are available.
     * <p>0 - HASH_HASHCODE : mostly bad
     * <p>1 - HASH_HASHCODE_SHIFT_REV : mostly bad
     * <p>2 - HASH_HASHCODE_LONG_REV : mostly bad
     * <p>3 - HASH_MD5 :  marginally better accuracy, much slower
     * <p>4 - HASH_PLUGGABLE_SHIFT : best blend of speed and accuracy
     * <p>This field is required. It is strongly recommended that you use "4".
     */
    @FieldConfig(codable = true, required = true)
    private int hash;

    /**
     * If {@link #bitset} is specified the you must populate
     * this field with the number of 0 bits in the initial bloom filter.
     */
    @FieldConfig(codable = true)
    private int bitsfree;

    public SeenFilterBasic() {
    }

    public SeenFilterBasic(int bits, int bitsper) {
        this(bits, bitsper, HASH_PLUGGABLE_SHIFT);
    }

    public SeenFilterBasic(int bits, int bitsper, int hash) {
        if (bits < 32) {
            throw new RuntimeException("invalid bits @ " + bits);
        }
        this.hash = hash;
        this.bits = (bits / 32) * 32;
        this.bitsfree = bits;
        this.bitsper = bitsper;
        this.bitset = new int[bits / 32];
    }

    public SeenFilterBasic<K> newInstance() {
        return new SeenFilterBasic<>(bits, bitsper, hash);
    }

    public SeenFilterBasic<K> newInstance(int bits) {
        return new SeenFilterBasic<>(bits > 0 ? bits : this.bits, bitsper, hash);
    }

    @Override
    public String toString() {
        return "SeenFilterBasic[" + bits + "," + bitset.length + "," + hash + "]";
    }

    /**
     * first stage byte set generator to feed hash algorithms
     */
    private static byte[] generatePreHash(Object o) {
        Class<?> clazz = o.getClass();
        if (clazz == Raw.class) {
            return ((Raw) o).toBytes();
        }
        if (clazz == Long.class) {
            return LessBytes.toBytes((Long) o);
        }
        return Raw.get(o.toString()).toBytes();
    }

    /**
     * call a given hash method for generating a single entry in a hash bit set
     */
    public static long customHash(Object o, int index, int hash) {
        switch (hash) {
            default:
            case HASH_HASHCODE:
                return o.hashCode();
            case HASH_HASHCODE_SHIFT_REV:
                int hc = o.hashCode();
                return (hc << 32) | LessBytes.reverseBits(hc);
            case HASH_HASHCODE_LONG_REV:
                long lhc = (long) o.hashCode();
                return lhc | LessBytes.reverseBits(lhc);
            case HASH_MD5:
                byte[] r1 = generatePreHash(o);
                byte[] r2 = new byte[r1.length];
                for (int i = 0; i < r1.length; i++) {
                    r2[r2.length - i - 1] = (byte) (r1[i] ^ index);
                }
                md5.reset();
                r1 = md5.digest(r1);
                r2 = md5.digest(r2);
                return (((long) PluggableHashFunction.hash(r1)) << 32L) | ((long) PluggableHashFunction.hash(r2));
            case HASH_PLUGGABLE_SHIFT:
                r1 = generatePreHash(o);
                r2 = new byte[r1.length];
                for (int i = 0; i < r1.length; i++) {
                    r2[r2.length - i - 1] = (byte) (r1[i] ^ index);
                }
                return (((long) PluggableHashFunction.hash(r1)) << 32) | ((long) PluggableHashFunction.hash(r2));
        }
    }

    /**
     * return number of bits backing this filter
     */
    public int getBits() {
        return bitset.length * 32;
    }

    public int[] getBitStore() {
        return bitset;
    }

    public int getBitCount() {
        return bits;
    }

    public int getBitsPerEntry() {
        return bitsper;
    }

    public int getHashMethod() {
        return hash;
    }

    /**
     * return used bit saturation (0-100) as a percent
     */
    public int getSaturation() {
        try {
            return 100 - (int) ((bitsfree * 100L) / (bitset.length * 32L));
        } catch (Exception ex) {
            System.out.println(hashCode() + " >> " + ex + " >> " + bits + " , " + bitsper + " , " + hash + " , " + bitsfree + " , " + bitset);
            return 0;
        }
    }

    /**
     * generate a single entry in hash offset set .
     * will be called with an index from 0-bitsper
     * to generate each bit in the hash set.
     * override this in subclasses and hash type will
     * be ignored.
     */
    public long generateHash(K o, int index) {
        return customHash(o, index, hash);
    }

    /**
     * generate a bit hash offset set
     */
    public long[] getHashSet(K o) {
        long[] bs = new long[bitsper];
        for (int i = 0; i < bitsper; i++) {
            bs[i] = Math.abs(generateHash(o, i));
        }
        return bs;
    }

    /**
     * return true (seen) if all bits set
     */
    public boolean checkHashSet(long[] bs) {
        for (long l : bs) {
            if (!getBit(Math.abs((int) (l % bits)))) {
                return false;
            }
        }
        return true;
    }

    /**
     * warning: like setHashSet but does not update bitsfee
     */
    public void updateHashSet(long[] bs) {
        for (int i = 0; i < bitsper; i++) {
            long hash = bs[i];
            int offset = (int) (hash % bits);
            int byteoff = offset / 32;
            long val = (1 << (offset % 32));
            bitset[byteoff] |= val;
        }
    }

    /**
     * set all bits from this hash offset set
     */
    public void setHashSet(long[] bs) {
        for (long l : bs) {
            setBit(Math.abs((int) (l % bits)));
        }
    }

    /**
     * sets this offset bit
     */
    public void setBit(int offset) {
        int byteoff = offset / 32;
        long val = (1 << (offset % 32));
        if ((bitset[byteoff] & val) == 0 && bitsfree > 0) {
            bitsfree--;
        }
        bitset[byteoff] |= val;
    }

    /**
     * returns true of this offset bit is set
     */
    public boolean getBit(int offset) {
        int byteoff = offset / 32;
        long val = (1 << (offset % 32));
        return (bitset[byteoff] & val) == val;
    }

    /**
     * return true if seen before
     */
    public boolean updateSeen(K o) {
        boolean allset = true;
        for (int i = 0; i < bitsper; i++) {
            long hash = Math.abs(generateHash(o, i));
            int offset = (int) (hash % bits);
            int byteoff = offset / 32;
            long val = (1 << (offset % 32));
            allset = allset & ((bitset[byteoff] & val) == 0);
            bitset[byteoff] |= val;
        }
        return allset;
    }

    public SeenFilterBasic<K> mergeSeen(SeenFilterBasic<?> merge) {
        return merge((SeenFilter<K>) merge);
    }

    @Override
    public SeenFilterBasic<K> merge(SeenFilter<K> merge) {
        if (!(merge instanceof SeenFilterBasic)) {
            throw new IllegalArgumentException(merge + " incompatible filter with " + this);
        }
        SeenFilterBasic<K> filterMerge = (SeenFilterBasic<K>) merge;
        if (!(filterMerge.hash == hash && filterMerge.bits == bits)) {
            throw new IllegalArgumentException(merge + " settings differ from " + this);
        }
        SeenFilterBasic<K> filterNew = new SeenFilterBasic<>();
        if (filterMerge.bits != bits || filterMerge.bitsper != bitsper || filterMerge.bitset.length != bitset.length) {
            throw new IllegalArgumentException("cannot merge dissimilar blooms");
        }
        filterNew.hash = hash;
        filterNew.bits = bits;
        filterNew.bitsfree = bits;
        filterNew.bitsper = bitsper;
        filterNew.bitset = new int[bitset.length];
        for (int i = 0; i < bitset.length; i++) {
            filterNew.bitset[i] = bitset[i] | filterMerge.bitset[i];
            long v = filterNew.bitset[i];
            for (int j = 0; j < 32; j++) {
                if ((v & 1) == 1) {
                    filterNew.bitsfree--;
                }
                v >>= 1;
            }
        }
        return filterNew;
    }

    @Override
    public void clear() {
        bitset = new int[bitset.length];
    }

    @Override
    public void setSeen(K o) {
        for (int i = 0; i < bitsper; i++) {
            long hash = Math.abs(generateHash(o, i));
            setBit((int) (hash % bits));
        }
    }

    @Override
    public boolean getSeen(K o) {
        for (int i = 0; i < bitsper; i++) {
            long hash = Math.abs(generateHash(o, i));
            if (!getBit((int) (hash % bits))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean getSetSeen(K o) {
        boolean seen = true;
        for (int i = 0; i < bitsper; i++) {
            long hash = Math.abs(generateHash(o, i));
            int bit = (int) (hash % bits);
            if (getBit(bit)) {
                continue;
            }
            setBit(bit);
            seen = false;
        }
        return seen;
    }

    @Override
    public void postDecode() {
        if (bits <= 0) {
            throw new RuntimeException("invalid bits @ 0");
        }
        if (bitset == null) {
            this.bitset = new int[bits / 32];
            this.bitsfree = bits;
        }
    }

    @Override
    public void preEncode() {
    }

}
