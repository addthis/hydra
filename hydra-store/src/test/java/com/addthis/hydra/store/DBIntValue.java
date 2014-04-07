package com.addthis.hydra.store;

import com.addthis.codec.Codec;

import java.nio.ByteBuffer;

public class DBIntValue implements Codec.BytesCodable {

    private Integer val;

    public DBIntValue(Integer val) {
        this.val = val;
    }

    public DBIntValue() {
    }

    public Integer getVal() {
        return val;
    }

    @Override
    public byte[] bytesEncode(long version) {
        return ByteBuffer.allocate(4).putInt(val).array();
    }

    @Override
    public void bytesDecode(byte[] b, long version) {
        val = b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DBIntValue that = (DBIntValue) o;

        if (val != null ? !val.equals(that.val) : that.val != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return val != null ? val.hashCode() : 0;
    }
}
