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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.binary.CodecBin2;
import com.addthis.hydra.store.kv.PageEncodeType;

import org.junit.Test;

import junit.framework.TestCase;

public class DataMapTest extends TestCase {

    @Test
    public void testSparseEncodeDecode() throws Exception {
        int size = 100;
        DataMap dataMap = new DataMap(size);
        long val = 1232432425l;
        dataMap.put("foo", ValueFactory.create(val));
        byte[] bytes = dataMap.bytesEncode(PageEncodeType.SPARSE.ordinal());
        DataMap decoded = new DataMap();
        decoded.bytesDecode(bytes, PageEncodeType.SPARSE.ordinal());
        ValueObject result = dataMap.getValue("foo");
        assertEquals(val, result.asLong().getLong());
        assertEquals(size, dataMap.getSize());
        assertEquals(size, decoded.getSize());
    }

    @Test
    public void testSparseEncodingSize() throws Exception {
        int size = 10000;
        DataMap dataMap = new DataMap(size);
        long initValue = 1232432425l;
        for (int i = 0; i < size; i++) {
            dataMap.put("key:" + i, ValueFactory.create(initValue + i));
        }
        CodecBin2 codec = CodecBin2.INSTANCE;
        byte[] codecBytes = codec.encode(dataMap);
        byte[] customBytes = dataMap.bytesEncode(PageEncodeType.SPARSE.ordinal());
        assertTrue(customBytes.length < codecBytes.length);
        DataMap dataMap1 = new DataMap(size);
        codec.decode(dataMap1, codecBytes);
        DataMap dataMap2 = new DataMap();
        dataMap2.bytesDecode(customBytes, PageEncodeType.SPARSE.ordinal());
        assertEquals(dataMap1.getValue("key:9997").asLong().getLong(), dataMap2.getValue("key:9997").asLong().getLong());
        assertEquals(size, dataMap1.getSize());
        assertEquals(size, dataMap2.getSize());
    }
}
