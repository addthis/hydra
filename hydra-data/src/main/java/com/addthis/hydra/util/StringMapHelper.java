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
package com.addthis.hydra.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.addthis.basis.kv.KVPair;
import com.addthis.basis.kv.KVPairs;
import com.addthis.basis.util.MapHelper;

public class StringMapHelper extends MapHelper<String, String> {

    public StringMapHelper() {
        this(new LinkedHashMap<String, String>());
    }

    public StringMapHelper(Map<String, String> map) {
        super(map);
    }

    public StringMapHelper add(KVPairs kv) {
        for (KVPair kvp : kv) {
            add(kvp.getKey(), kvp.getValue());
        }
        return this;
    }

    public KVPairs createKVPairs() {
        KVPairs kv = new KVPairs();
        for (Entry<String, String> e : map().entrySet()) {
            kv.putValue(e.getKey(), e.getValue());
        }
        return kv;
    }

    public StringMapHelper put(String key, Object val) {
        if (val != null) {
            super.add(key, val.toString());
        }
        return this;
    }

    public String toLog() {
        StringBuffer sb = new StringBuffer();
        for (Entry<String, String> kv : map().entrySet()) {
            sb.append(kv.getKey() + "=" + kv.getValue() + " ");
        }
        return sb.toString();
    }
}
