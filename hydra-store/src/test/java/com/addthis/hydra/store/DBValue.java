package com.addthis.hydra.store;

import com.addthis.codec.Codec;

public class DBValue implements Codec.BytesCodable {

    private String val;

    public DBValue(String val) {
        this.val = val;
    }

    public String getVal() {
        return val;
    }

    @Override
    public byte[] bytesEncode() {
        return val.getBytes();
    }

    @Override
    public void bytesDecode(byte[] b) {
        val = new String(b);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DBValue dbValue = (DBValue) o;

        if (val != null ? !val.equals(dbValue.val) : dbValue.val != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return val != null ? val.hashCode() : 0;
    }
}
