package com.addthis.hydra.data.tree.prop;

import com.addthis.codec.CodecBin2;
import junit.framework.TestCase;

public class DataTimeTest extends TestCase {


    public void testEncodeDecode() throws Exception {
        DataTime time = new DataTime();
        long first = System.currentTimeMillis();
        long last = first + 100000;
        time.setFirst(first);
        time.setLast(last);
        byte[] encoded = time.bytesEncode();
        CodecBin2 codec = new CodecBin2();
        byte[] codecEncoded = codec.encode(time);
        DataTime timeDecoded = new DataTime();
        timeDecoded.bytesDecode(encoded);
        assertTrue(timeDecoded.getValue("first").asLong().getLong() == time.getValue("first").asLong().getLong());
        assertTrue(timeDecoded.getValue("last").asLong().getLong() == time.getValue("last").asLong().getLong());
        assertTrue(encoded.length < codecEncoded.length);
    }
}
