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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.zip.GZIPOutputStream;

import com.addthis.basis.util.MemoryCounter;
import com.addthis.basis.util.Numbers;

import com.addthis.codec.Codec;
import com.addthis.codec.binary.CodecBin2;
import com.addthis.codec.json.CodecJSON;

import org.junit.Test;


public class TestSeenFilterSize {

    private static final Codec codec     = CodecBin2.INSTANCE;
    private static final Codec codecJSON = CodecJSON.INSTANCE;

    @Test
    public void noop() {
    }

    // http://piotrga.wordpress.com/2009/06/08/howto-compress-or-decompress-byte-array-in-java/
    public static byte[] compress(byte[] content) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            gzipOutputStream.write(content);
            gzipOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.printf("Compression ratio %f\n",
                          (1.0f * content.length / byteArrayOutputStream.size()));
        return byteArrayOutputStream.toByteArray();
    }

    public static void main(String[] args) throws Exception {
        //SeenFilterBasic(int bits, int bitsper, int hash)
        int bits = 128;
        int bitsper = 4;
        int hash = 4;

        if (args.length > 0) bits = Numbers.parseInt(args[0], bits, 10);
        if (args.length > 1) bitsper = Numbers.parseInt(args[1], bitsper, 10);
        if (args.length > 2) hash = Numbers.parseInt(args[2], hash, 10);

        SeenFilter<String> sf = new SeenFilterBasic<String>(bits, bitsper, hash);
        long memSize = MemoryCounter.estimateSize(sf);
        System.out.println("MemorySize: " + memSize);
        byte[] encodedBytes = codec.encode(sf);
        System.out.println("Encoded Size: " + encodedBytes.length);
        encodedBytes = null; // gc now...
        System.out.println("Encoded JSON size: " + codecJSON.encode(sf).length);

        // not meaningful without data in the filter
        //System.out.println("gzip encoded size: " 
    }

}
