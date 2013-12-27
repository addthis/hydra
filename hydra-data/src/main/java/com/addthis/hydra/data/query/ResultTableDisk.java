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

import java.io.File;
import java.io.IOException;

import java.util.Comparator;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.io.DataChannelCodec.ClassIndexMap;
import com.addthis.bundle.io.DataChannelCodec.FieldIndexMap;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.hydra.data.io.DiskBackedList2;
import com.addthis.hydra.data.io.DiskBackedList2.ItemCodec;


/**
 * a disk-backed implementation of DataTable using DiskBackedList.
 * designed to be temporary and never re-opened.  does not persist
 * the index maps, which would be required to achieve this.
 */
public class ResultTableDisk extends ResultTable implements ItemCodec<Bundle> {

    private final FieldIndexMap fim = DataChannelCodec.createFieldIndexMap();
    private final ClassIndexMap cim = DataChannelCodec.createClassIndexMap();
    private final DiskBackedList2<Bundle> diskList;
    private final File diskFile;
    private boolean closed;

    public ResultTableDisk(DataTableFactory factory, File tmpFile) throws IOException {
        super(factory, new DiskBackedList2<Bundle>(null));
        diskList = (DiskBackedList2<Bundle>) getBackingList();
        diskList.setCodec(this);
        this.diskFile = tmpFile;
    }

    @Override
    public String toString() {
        return "(RDB:" + diskFile + ":" + diskList.size() + ")";
    }

    protected void finalize() {
        if (!closed) {
            System.err.println("finalizing ResultDiskBacked via delete rows=" + size() + " @ " + diskFile);
            delete();
        }
    }

    public void delete() {
        close();
        if (diskFile.exists()) {
            diskFile.delete();
        }
    }

    public void close() {
        if (!closed) {
            try {
                diskList.close();
                closed = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void append(DataTable result) {
        if (result instanceof ResultTableDisk) {
            try {
                diskList.addEncodedData(((ResultTableDisk) result).diskList.getEncodedData());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            for (Bundle row : result) {
                append(row);
            }
        }
    }

    @Override
    public void sort(final Comparator<Bundle> comp) {
        try {
            diskList.sort(new Comparator<Bundle>() {
                @Override
                public int compare(Bundle o1, Bundle o2) {
                    return comp.compare(o1, o2);
                }
            });
        } catch (IOException io) {
            System.err.println("io exception during sort");
        }

    }

    @Override
    public Bundle decode(byte[] row) throws IOException {
        return DataChannelCodec.decodeBundle(createBundle(), row, fim, cim);
    }

    @Override
    public byte[] encode(Bundle row) throws IOException {
        return DataChannelCodec.encodeBundle(row, fim, cim);
    }
}
