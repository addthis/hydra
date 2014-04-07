package com.addthis.hydra.data.tree.prop;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.CodecBin2;
import com.addthis.hydra.store.kv.KeyCoder;

import org.junit.Test;

import junit.framework.TestCase;

public class DataMapTest extends TestCase {

    @Test
    public void testSparseEncodeDecode() throws Exception {
        DataMap dataMap = new DataMap(100);
        long val = 1232432425l;
        dataMap.put("foo", ValueFactory.create(val));
        byte[] bytes = dataMap.bytesEncode(KeyCoder.EncodeType.SPARSE.ordinal());
        DataMap decoded = new DataMap();
        decoded.bytesDecode(bytes, KeyCoder.EncodeType.SPARSE.ordinal());
        ValueObject result = dataMap.getValue("foo");
        assertEquals(val, result.asLong().getLong());
    }

    @Test
    public void testSparseEncodingSize() throws Exception {
        int size = 10000;
        DataMap dataMap = new DataMap(size);
        long initValue = 1232432425l;
        for (int i = 0; i < size; i++) {
            dataMap.put("key:" + i, ValueFactory.create(initValue + i));
        }
        CodecBin2 codec = new CodecBin2();
        byte[] codecBytes = codec.encode(dataMap);
        byte[] customBytes = dataMap.bytesEncode(KeyCoder.EncodeType.SPARSE.ordinal());
        assertTrue(customBytes.length < codecBytes.length);
        DataMap dataMap1 = new DataMap(size);
        codec.decode(dataMap1, codecBytes);
        DataMap dataMap2 = new DataMap(size);
        dataMap2.bytesDecode(customBytes, KeyCoder.EncodeType.SPARSE.ordinal());
        assertEquals(dataMap1.getValue("key:9997").asLong().getLong(), dataMap2.getValue("key:9997").asLong().getLong());
    }
}
