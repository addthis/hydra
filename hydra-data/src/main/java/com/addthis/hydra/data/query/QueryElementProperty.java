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

import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.tree.DataTreeNode;
import com.addthis.hydra.data.tree.ReadTreeNode;

import org.apache.commons.lang3.mutable.MutableInt;

/**
 * For retrieval of node intrinsic properties by name/key.
 * <p/>
 * hits, nodes, count, mem, json
 *
 * @user-reference
 */
public class QueryElementProperty implements Codable {

    // output column this element is bound to 'show' (or null if dropped)
    @FieldConfig(codable = true)
    private String column;
    @FieldConfig(codable = true)
    public BoundedValue key;

    private BundleField field;

    public QueryElementProperty parse(String tok, MutableInt nextColumn) {
        if (tok.startsWith("+")) {
            // show = true;
            column = Integer.toString(nextColumn.intValue());
            nextColumn.increment();
            tok = tok.substring(1);
        }
        key = new BoundedValue().parse(tok, nextColumn);
        return this;
    }

    void toCompact(StringBuilder sb) {
        if (show()) {
            sb.append("+");
        }
        if (key != null) {
            key.toCompact(sb);
        }
    }

    public String column() {
        return column();
    }

    public boolean show() {
        return column != null;
    }

    public BundleField field(BundleFormat format) {
        if (field == null) {
            field = format.getField(column);
        }
        return field;
    }

    public ValueObject getValue(DataTreeNode node) {
        if (key != null && key.name != null) {
            if (key.name.equals("json")) {
                try {
                    return ValueFactory.create(CodecJSON.encodeString(node));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Long value = null;
            if (key.name.equals("hits")) {
                value = node.getCounter();
            } else if (key.name.equals("count")) {
                value = node.getCounter();
            } else if (key.name.equals("nodes")) {
                value = (long) node.getNodeCount();
            } else if (key.name.equals("mem")) {
                if (node instanceof ReadTreeNode) {
                    value = (long) ((ReadTreeNode) node).getWeight();
                } else {
                    value = 0L;
                }
            }
            if (value != null && key.validate(value)) {
                return ValueFactory.create(value);
            }
        }
        return null;
    }
}
