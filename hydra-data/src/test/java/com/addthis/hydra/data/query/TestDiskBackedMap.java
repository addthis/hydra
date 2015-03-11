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
package com.addthis.hydra.data.query;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;

import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.apache.commons.lang3.ArrayUtils;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDiskBackedMap {

    private final DataChannelCodec.ClassIndexMap classMap = DataChannelCodec.createClassIndexMap();
    private final DataChannelCodec.FieldIndexMap fieldMap = DataChannelCodec.createFieldIndexMap();

    private static class MergedRow implements DiskBackedMap.DiskObject {

        MergedRow() {
            mergedRow = new ValueObject[5];
        }

        ValueObject[] mergedRow;
        int           merged;

        @Override
        public byte[] toBytes() {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] result = null;
            try {
                bos.write(LessBytes.toBytes(merged));
                int pos = 0;
                ListBundleFormat format = new ListBundleFormat();
                ListBundle listBundle = new ListBundle(format);
                for (ValueObject valueObject : mergedRow) {
                    listBundle.setValue(format.getField("" + pos), valueObject);
                    pos++;
                }
                bos.write(DataChannelCodec.encodeBundle(listBundle));
                result = bos.toByteArray();
                bos.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return result;
        }
    }

    private static class MergedRowFactory implements DiskBackedMap.DiskObjectFactory {

        @Override
        public DiskBackedMap.DiskObject fromBytes(byte[] bytes) {
            MergedRow mergedRow = new MergedRow();
            mergedRow.merged = LessBytes.toInt(ArrayUtils.subarray(bytes, 0, 8));
            ListBundleFormat lbf = new ListBundleFormat();
            ListBundle listBundle = new ListBundle(lbf);
            try {
                DataChannelCodec.decodeBundle(listBundle,
                                              ArrayUtils.subarray(bytes, Integer.SIZE / 8,
                                                                  bytes.length));
                mergedRow.mergedRow = new ValueObject[5];
                for (int i = 0; i != 5; i++) {
                    mergedRow.mergedRow[i] = listBundle.getValue(lbf.getField("" + i));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return mergedRow;
        }
    }

    @Test
    public void testDiskBackedMap() {
        // Create a diskbackedmap
        MergedRowFactory mergedRowFactory = new MergedRowFactory();
        LessFiles.deleteDir(new File("/tmp/testDiskBackedMap"));
        DiskBackedMap<MergedRow> diskBackedMap = new DiskBackedMap<>("/tmp/testDiskBackedMap",
                mergedRowFactory, 16 * 1024 * 1024L);

        MergedRow row1 = new MergedRow();
        row1.mergedRow[0] = ValueFactory.create(1.7);
        row1.mergedRow[1] = ValueFactory.create("Hello");
        row1.mergedRow[2] = ValueFactory.create((long) 1);
        row1.mergedRow[3] = ValueFactory.create(new byte[]{5, 6, 7, 100});
        row1.mergedRow[4] = ValueFactory.create(700);

        diskBackedMap.put("row1", row1);

        MergedRow row2 = diskBackedMap.get("row1");

        // Compare the values
        assertTrue(row1.mergedRow[0].asDouble().getDouble() == row2.mergedRow[0].asDouble().getDouble());
        assertTrue(row1.mergedRow[1].asString().asNative().equals(row2.mergedRow[1].asString().asNative()));
        assertTrue(row1.mergedRow[2].asLong().getLong() == row2.mergedRow[2].asLong().getLong());
        byte[] row2bytes = row1.mergedRow[3].asBytes().asNative();
        assertTrue(row2bytes.length == 4);
        assertTrue(row2bytes[0] == 5);
        assertTrue(row2bytes[1] == 6);
        assertTrue(row2bytes[2] == 7);
        assertTrue(row2bytes[3] == 100);
        assertTrue(row1.mergedRow[4].asLong().getLong() == row2.mergedRow[4].asLong().getLong());

        try {
            diskBackedMap.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        // Check that the directory has been removed
        assertFalse((new File("/tmp/testDiskBackedMap")).exists());
    }
}
