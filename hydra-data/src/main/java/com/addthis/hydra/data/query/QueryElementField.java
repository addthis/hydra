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

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.codec.CodecBin2;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.ReadNode;
import com.addthis.hydra.data.tree.TreeNodeData;

import org.apache.commons.lang3.mutable.MutableInt;

/**
 * For retrieval of node data attachments by name/key
 *
 * @user-reference
 */
public class QueryElementField implements Codec.Codable {

    private static BoundedValue[] memKey = new BoundedValue[0];

    // output column this element is bound to 'show' (or null if dropped)
    @Codec.Set(codable = true)
    private String column;
    @Codec.Set(codable = true)
    public String name;
    @Codec.Set(codable = true)
    public BoundedValue[] keys;

    private BundleField field;

    public QueryElementField parse(String tok, MutableInt nextColumn) {
        if (tok.startsWith("+")) {
            // show = true;
            column = Integer.toString(nextColumn.intValue());
            nextColumn.increment();
            tok = tok.substring(1);
        } else if (tok.startsWith("?")) {
            // show = true;
            column = Integer.toString(nextColumn.intValue());
            nextColumn.increment();
            tok = tok.substring(1);
            keys = memKey;
        }
        String[] kv = Strings.splitArray(Bytes.urldecode(tok), "=");
        if (kv.length == 2) {
            name = kv[0];
            String[] keyarr = Strings.splitArray(kv[1], ",");
            keys = new BoundedValue[keyarr.length];
            for (int i = 0; i < keyarr.length; i++) {
                keys[i] = new BoundedValue().parse(keyarr[i], nextColumn);
            }
        } else if (kv.length == 1) {
            name = kv[0];
            if (keys == null) {
                keys = new BoundedValue[1];
                keys[0] = new BoundedValue();
            }
        }
        return this;
    }

    void toCompact(StringBuilder sb) {
        if (mem()) {
            sb.append("?");
        } else if (show()) {
            sb.append("+");
        }
        sb.append(Bytes.urlencode(name != null ? name : ""));
        if (keys != null && !mem() && keys.length > 0) {
            sb.append("=");
            for (BoundedValue bv : keys) {
                bv.toCompact(sb);
            }
        }
    }

    public boolean show() {
        return column != null;// show != null && show.booleanValue();
    }

    public boolean mem() {
        return keys != null && keys.length == 0;
    }

    public BundleField field(BundleFormat format) {
        if (field == null) {
            field = format.getField(column);
        }
        return field;
    }

    public List<ValueObject> getValues(ReadNode node) {
        if (keys == null) {
            return new ArrayList<>();
        }
        ArrayList<ValueObject> ret = new ArrayList<>(keys.length);
        synchronized (node) {
            TreeNodeData<?> actor = node.getData(name);
            if (actor != null) {
                if (keys.length == 0) {
                    int size = -1;
                    try {
                        size = CodecBin2.encodeBytes(actor).length;
                    } catch (Exception e) {
                    } finally {
                        ret.add(ValueFactory.create(size));
                    }
                    return ret;
                }
                for (int i = 0; i < keys.length; i++) {
                    ValueObject qv = actor.getValue(keys[i].name);
                    if (keys[i].bounded) {
                        try {
                            ret.add(keys[i].validate(qv.asLong().getLong()) ? qv : null);
                        } catch (NumberFormatException ex) {
                            ret.add(null);
                        }
                    } else {
                        if (qv == null) {
                            ret.add(null);
                            continue;
                        }
                        if (qv.getObjectType() == ValueObject.TYPE.ARRAY) {
                            for (ValueObject o : qv.asArray()) {
                                ret.add(o);
                            }
                        } else {
                            ret.add(qv);
                        }
                    }
                }
                return ret;
            }
        }
        for (int i = 0; i < keys.length; i++) {
            ret.add(null);
        }
        return ret;
    }
}
