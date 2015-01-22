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

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleException;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONException;
import com.addthis.maljson.JSONObject;

public class JSONBundleizer extends NewlineBundleizer {

    @Override
    public void open() { }

    @Override
    public Bundle bundleize(Bundle next, String line) {
        BundleFormat format = next.getFormat();
        try {
            JSONObject json = new JSONObject(line);
            for (String key : json.keySet()) {
                next.setValue(format.getField(key), getJSONValue(json, key));
            }
        } catch (JSONException | BundleException ex) {
            throw new DataChannelError(ex);
        }
        return next;
    }

    //copied with slight changes from DataSourceJSON

    private ValueObject getJSONValue(JSONObject json, String key) throws BundleException {
        try {
            Object raw = json.opt(key);
            if (raw == null) {
                return null;
            }
            Class<?> clazz = raw.getClass();

                /* unwrap JSONObject to ValueMap */
            if (clazz == JSONObject.class) {
                // TODO
                throw new UnsupportedOperationException("TODO");
            } else if (clazz == JSONArray.class) {
                // TODO: still only one dimension supported
                JSONArray jarr = (JSONArray) raw;
                ValueArray arr = ValueFactory.createArray(jarr.length());
                for (int i = 0; i < jarr.length(); i++) {
                    arr.add(i, createPrimitiveBundle(jarr.opt(i).getClass(), jarr.opt(i)));
                }
                return arr;
            } else {
                return createPrimitiveBundle(clazz, raw);
            }
        } catch (Exception ex) {
            throw new BundleException(ex);
        }
    }

    private ValueObject createPrimitiveBundle(Class<?> clazz, Object raw) {
        // it would be cool to not do this twice, but I gave up fighting with generics
        //Class<?> clazz = raw.getClass();
        if (clazz == Integer.class) {
            return ValueFactory.create(((Integer) raw).longValue());
        } else if (clazz == Long.class) {
            return ValueFactory.create(((Long) raw).longValue());
        } else if (clazz == Float.class) {
            return ValueFactory.create(((Float) raw).doubleValue());
        } else if (clazz == Double.class) {
            return ValueFactory.create(((Double) raw).doubleValue());
        } else {
            return ValueFactory.create(raw.toString());
        }
    }
}
