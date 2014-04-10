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

package com.addthis.hydra.data.query.op;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.Iterator;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleException;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.DiskBackedMap;
import com.addthis.hydra.data.query.op.merge.MergedValue;

public class MergedRow implements DiskBackedMap.DiskObject, Bundle {

    private final MergedValue[] conf;
    private final Bundle mergedBundle;

    int numMergedRows = 0;

    MergedRow(MergedValue[] conf, Bundle backingBundle) {
        this.conf = conf;
        this.mergedBundle = backingBundle;
    }

    void merge(Bundle row) {
        numMergedRows += 1;
        for (MergedValue map : conf) {
            if (map == null) {
                continue;
            }
            map.merge(row, this);
        }
    }

    Bundle emit() {
        for (MergedValue map : conf) {
            if (map == null) {
                continue;
            }
            map.emit(this);
        }
        return mergedBundle;
    }

    public int getMergedCount() {
        return numMergedRows;
    }

    @Override
    public byte[] toBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] result;
        try {
            bos.write(Bytes.toBytes(numMergedRows));
            ListBundleFormat format = new ListBundleFormat();
            ListBundle listBundle = new ListBundle(format);
            emit();
            int i = 0;
            for (MergedValue map : conf) {
                if (map == null) {
                    continue;
                }
                BundleField toField = format.getField("" + i);
                i += 1;
                listBundle.setValue(toField, getValue(map.getTo()));
            }
            bos.write(DataChannelCodec.encodeBundle(listBundle));
            result = bos.toByteArray();
            bos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public ValueObject getValue(BundleField field) throws BundleException {
        return mergedBundle.getValue(field);
    }

    @Override
    public void setValue(BundleField field, ValueObject value) throws BundleException {
        mergedBundle.setValue(field, value);
    }

    @Override
    public void removeValue(BundleField field) throws BundleException {
        mergedBundle.removeValue(field);
    }

    @Override
    public BundleFormat getFormat() {
        return mergedBundle.getFormat();
    }

    @Override
    public int getCount() {
        return mergedBundle.getCount();
    }

    @Override
    public Bundle createBundle() {
        return mergedBundle.createBundle();
    }

    @Override
    public Iterator<BundleField> iterator() {
        return mergedBundle.iterator();
    }
}
