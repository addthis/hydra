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

import java.io.IOException;

import com.addthis.basis.util.Bytes;

import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.io.DataChannelCodec;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.DiskBackedMap;
import com.addthis.hydra.data.query.op.merge.BundleMapConf;
import com.addthis.hydra.data.query.op.merge.MergedValue;

import org.apache.commons.lang3.ArrayUtils;

public class MergedRowFactory implements DiskBackedMap.DiskObjectFactory {

    private final BundleMapConf<MergedValue> conf[];
    private final ListBundleFormat format;

    MergedRowFactory(BundleMapConf<MergedValue>[] conf, ListBundleFormat format) {
        this.conf = conf;
        this.format = format;
    }

    @Override
    public DiskBackedMap.DiskObject fromBytes(byte[] bytes) {
        MergedRow mergedRow = new MergedRow(conf, format);
        mergedRow.numMergedRows = Bytes.toInt(ArrayUtils.subarray(bytes, 0, Integer.SIZE / 8));
        ListBundleFormat format = new ListBundleFormat();
        ListBundle listBundle = new ListBundle(format);
        try {
            DataChannelCodec.decodeBundle(listBundle, ArrayUtils.subarray(bytes, Integer.SIZE / 8, bytes.length));
            int i = 0;
            for (BundleMapConf<MergedValue> map : conf) {
                if () // TODO : recreate ops that need merge count?
                ValueObject value = listBundle.getValue(format.getField("" + i));
                i += 1;
                map.getOp().merge(value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return mergedRow;
    }
}
