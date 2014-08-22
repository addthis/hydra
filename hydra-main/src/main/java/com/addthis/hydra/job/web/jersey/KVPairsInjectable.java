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
package com.addthis.hydra.job.web.jersey;

import javax.ws.rs.core.MultivaluedMap;

import java.util.List;
import java.util.Map;

import com.addthis.basis.kv.KVPairs;

import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;

public class KVPairsInjectable extends AbstractHttpContextInjectable<KVPairs> {

    @Override
    public KVPairs getValue(HttpContext c) {
        KVPairs pairs = new KVPairs();
        MultivaluedMap<String, String> params = c.getRequest().getFormParameters();
        for (Map.Entry<String, List<String>> param : params.entrySet()) {
            String key = param.getKey();
            Object value = param.getValue().iterator().next();
            pairs.putValue(key, value.toString());
        }
        return pairs;
    }
}
