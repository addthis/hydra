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
package com.addthis.hydra.task.output.tree;

import java.util.ArrayList;
import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.data.query.BoundedValue;
import com.addthis.hydra.data.query.QueryElementField;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.DataTreeNodeActor;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
public class PathQueryElementField extends QueryElementField {

    private static Logger log = LoggerFactory.getLogger(PathQueryElementField.class);

    @Codec.Set(codable = true)
    public ResolvableBoundedValue keys[];


    public void resolve(TreeMapper mapper) {
        if (keys != null) {
            for (ResolvableBoundedValue key : keys) {
                key.resolve(mapper);
            }
        }
    }

    public List<ValueObject> getValues(DataTreeNode node, TreeMapState state) {
        if (keys == null) {
            return new ArrayList<ValueObject>();
        }
        ArrayList<ValueObject> ret = new ArrayList<ValueObject>(keys.length);

        DataTreeNodeActor actor = node.getData(name);
        if (actor != null && keys != null) {
            for (int i = 0; i < keys.length; i++) {
                String name = keys[i].name;
                if (keys[i].key != null) {
                    // TODO: filter?
                    name = ValueUtil.asNativeString(keys[i].getKeyValue(state.getBundle()));
                }
                ValueObject qv = actor.onValueQuery(name);
                if (keys[i].bounded) {
                    try {
                        ret.add(keys[i].validate(qv.asLong().getLong()) ? qv : null);
                    } catch (NumberFormatException ex) {
                        ret.add(null);
                    }
                } else {
                    ret.add(qv);
                }
            }
            return ret;
        }

        for (int i = 0; i < keys.length; i++) {
            ret.add(null);
        }
        return ret;
    }

    public static class ResolvableBoundedValue extends BoundedValue {

        @Codec.Set(codable = true)
        public String key;

        private BundleField bundleField;

        public void resolve(TreeMapper mapper) {
            bundleField = mapper.getFormat().getField(key);
        }

        /** */
        public final ValueObject getKeyValue(Bundle p) {
            try {
                ValueObject pv = null;
                if (bundleField != null) {
                    pv = p.getValue(bundleField);
                }
                return pv;
            } catch (NullPointerException ex) {
                try {
                    log.warn("NPE: keyAccess=" + bundleField + " p=" + p + " in " + CodecJSON.encodeString(this));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                throw ex;
            }
        }
    }
}
