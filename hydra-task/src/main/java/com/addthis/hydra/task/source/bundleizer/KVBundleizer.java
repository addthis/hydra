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
package com.addthis.hydra.task.source.bundleizer;

import com.addthis.basis.kv.KVPair;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueFactory;

/**
 * chops strings into kv pairs (url encoded)
 *
 */
public class KVBundleizer extends NewlineBundleizer {

    @Override
    public Bundle bundleize(Bundle next, String line) {
        BundleFormat format = next.getFormat();
        int i = 0;
        int j = line.indexOf('&');
        while (j >= 0) {
            if (j > 0) {
                KVPair kv = KVPair.parsePair(line.substring(i, j));
                next.setValue(format.getField(kv.getKey()), ValueFactory.create(kv.getValue()));
            }
            i = j + 1;
            j = line.indexOf('&', i);
        }
        KVPair kv = KVPair.parsePair(line.substring(i));
        next.setValue(format.getField(kv.getKey()), ValueFactory.create(kv.getValue()));
        return next;
    }

}
