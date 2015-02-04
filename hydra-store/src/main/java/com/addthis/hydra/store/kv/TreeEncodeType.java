package com.addthis.hydra.store.kv;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;

public enum TreeEncodeType {
    BIT32(32, Integer.MAX_VALUE, "SPARSE"),
    BIT64(64, Integer.MAX_VALUE, "LONGIDS");

    private final int bits;

    private final long max;

    @Nonnull
    private final String defaultPageTypeString;

    private PageEncodeType defaultPageType;

    public int getBytes() { return bits / 8; }

    public long getMax() {
        return max;
    }

    public PageEncodeType getDefaultPageType() {
        if (defaultPageType == null) {
            defaultPageType = PageEncodeType.valueOf(defaultPageTypeString);
        }
        return defaultPageType;
    }

    private TreeEncodeType(int bits, long max, String pageType) {
        this.bits = bits;
        this.max = max;
        this.defaultPageTypeString = pageType;
    }

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
