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
package com.addthis.hydra.data.tree.prop;

import com.addthis.codec.binary.CodecBin2;
import com.addthis.hydra.store.kv.KeyCoder;

import junit.framework.TestCase;

public class DataTimeTest extends TestCase {


    public void testSparseEncodeDecode() throws Exception {
        DataTime time = new DataTime();
        long first = System.currentTimeMillis();
        long last = first + 100000;
        time.setFirst(first);
        time.setLast(last);
        byte[] encoded = time.bytesEncode(KeyCoder.EncodeType.SPARSE.ordinal());
        CodecBin2 codec = CodecBin2.INSTANCE;
        byte[] codecEncoded = codec.encode(time);
        DataTime timeDecoded = new DataTime();
        timeDecoded.bytesDecode(encoded, KeyCoder.EncodeType.SPARSE.ordinal());
        assertTrue(timeDecoded.getValue("first").asLong().getLong() == time.getValue("first").asLong().getLong());
        assertTrue(timeDecoded.getValue("last").asLong().getLong() == time.getValue("last").asLong().getLong());
        assertTrue(encoded.length < codecEncoded.length);
    }
}
