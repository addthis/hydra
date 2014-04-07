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

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.DiskBackedMap;
import com.addthis.hydra.data.query.op.merge.BundleMapConf;
import com.addthis.hydra.data.query.op.merge.MergedValue;

class MergedRow implements DiskBackedMap.DiskObject {

    private final BundleMapConf<MergedValue> conf[];
    private final ListBundleFormat format;

    int numMergedRows = 0;

    MergedRow(BundleMapConf<MergedValue>[] conf, ListBundleFormat format) {
        this.conf = conf;
        this.format = format;
    }

    void merge(Bundle row) {
        numMergedRows += 1;
        for (BundleMapConf<MergedValue> map : conf) {
            if (map == null) {
                continue;
            }
            ValueObject lval = row.getValue(map.getFrom());
            map.getOp().merge(lval);
        }
    }

    Bundle emit() {
        Bundle nl = new ListBundle(format);
        for (BundleMapConf<MergedValue> map : conf) {
            if (map == null) {
                continue;
            }
            nl.setValue(map.getTo(), map.getOp().emit());
        }
        return nl;
    }

    @Override
    public byte[] toBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] result;
        try {
            bos.write(Bytes.toBytes(numMergedRows));
            ListBundleFormat format = new ListBundleFormat();
            ListBundle listBundle = new ListBundle(format);
            int i = 0;
            for (BundleMapConf<MergedValue> map : conf) {
                if (map == null) {
                    continue;
                }
                BundleField toField = format.getField("" + i);
                i += 1;
                listBundle.setValue(toField, map.getOp().emit());
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
