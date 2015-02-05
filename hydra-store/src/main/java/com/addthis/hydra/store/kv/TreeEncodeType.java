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
package com.addthis.hydra.store.kv;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;

public enum TreeEncodeType {
    BIT32(32, Integer.MAX_VALUE, "SPARSE"),
    BIT64(64, Long.MAX_VALUE, "LONGIDS");

    /**
     * Number of bits in the serialized representation
     */
    private final int bits;

    /**
     * Largest value that can be stored in this representation.
     */
    private final long max;

    @Nonnull
    private final String defaultPageTypeString;

    /**
     * The default page encoding type associated with
     * this tree encoding type. The default page encoding
     * is selected when creating new trees.
     */
    private PageEncodeType defaultPageType;

    private TreeEncodeType(int bits, long max, String pageType) {
        this.bits = bits;
        this.max = max;
        this.defaultPageTypeString = pageType;
    }

    /**
     * Number of bytes used to store a node identifier.
     *
     * @return number of bytes used to store a node identifier.
     */
    public int getBytes() { return bits / 8; }

    /**
     * Maximum value of an node identifier .
     *
     * @return maximum value of an node identifier .
     */
    public long getMax() {
        return max;
    }

    public PageEncodeType getDefaultPageType() {
        if (defaultPageType == null) {
            defaultPageType = PageEncodeType.valueOf(defaultPageTypeString);
        }
        return defaultPageType;
    }

    /**
     * Converts a node identifier into its serialized representation.
     *
     * @param value node identifier number
     * @return serialized representation of value
     */
    public byte[] idToBytes(long value) {
        switch (bits) {
            case 32:
                return Bytes.toBytes((int) value);
            case 64:
                return Bytes.toBytes(value);
            default:
                throw new IllegalStateException("Unknown state " + this);
        }
    }

    /**
     * Retrieves a node identifier from a byte array
     * or returns a default value if unsuccessful.
     *
     * @param input          serialized representation of identifier
     * @param offset         start position in input
     * @param defaultValue   if enable to deserialize then return this value
     * @return deserialized node identifier
     */
    public long getId(byte[] input, int offset, long defaultValue) {
        switch (bits) {
            case 32:
                return (long) Bytes.toInt(input, offset, (int) defaultValue);
            case 64:
                return Bytes.toLong(input, offset, defaultValue);
            default:
                throw new IllegalStateException("Unknown state " + this);
        }
    }

    /**
     * Retrieves a node identifier from an input stream.
     *
     * @param is input stream
     * @return deserialized node identifier
     * @throws IOException
     */
    public long getId(InputStream is) throws IOException {
        switch (bits) {
            case 32:
                return (long) Bytes.readInt(is);
            case 64:
                return Bytes.readLong(is);
            default:
                throw new IllegalStateException("Unknown state " + this);
        }
    }

    /**
     * Writes a node identifier to an output stream.
     *
     * @param out    output stream
     * @param value  node identifier
     * @throws IOException
     */
    public void writeId(OutputStream out, long value) throws IOException {
        switch (bits) {
            case 32:
                Bytes.writeInt((int) value, out);
                break;
            case 64:
                Bytes.writeLong(value, out);
                break;
            default:
                throw new IllegalStateException("Unknown state " + this);
        }
    }

    public static TreeEncodeType fromString(String input) {
        if (input == null) {
            throw new NullPointerException("input must be a non-null value");
        }
        return TreeEncodeType.valueOf(input.toUpperCase());
    }

    /**
     * If we are creating a tree from scratch then use this tree encoding type.
     * @return
     */
    public static TreeEncodeType defaultType() {
        return BIT64;
    }

    /**
     * If we encounter an existing tree that is unlabeled then use this tree encoding type.
     * @return
     */
    public static TreeEncodeType implicitType() {
        return BIT32;
    }

}
