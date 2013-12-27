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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.hydra.data.query.FieldValueList;
import com.addthis.hydra.data.query.QueryElement;
import com.addthis.hydra.data.query.QueryElementProperty;
import com.addthis.hydra.data.tree.DataTreeNode;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * @user-reference
 */
public class PathQueryElement extends QueryElement {

    private static Logger log = LoggerFactory.getLogger(PathQueryElement.class);

    @Codec.Set(codable = true)
    private ArrayList<PathQueryElementField> field;

    public void resolve(final TreeMapper mapper) {
        if (field != null) {
            for (int i = 0; i < field.size(); i++) {
                field.get(i).resolve(mapper);
            }
        }
    }

    public int update(FieldValueList fvlist, DataTreeNode tn, TreeMapState state) {
        if (tn == null) {
            return 0;
        }
        int updates = 0;
        if (getNode() != null && getNode().show()) {
            fvlist.push(getNode().field(fvlist.getFormat()), ValueFactory.create(tn.getName()));
            updates++;
        }
        if (getProp() != null) {
            for (QueryElementProperty p : getProp()) {
                ValueObject val = p.getValue(tn);
                if (val == null && !emptyok()) {
                    fvlist.rollback();
                    return 0;
                }
                if (p.show()) {
                    fvlist.push(p.field(fvlist.getFormat()), val);
                    updates++;
                }
            }
        }
        if (field != null) {
            for (PathQueryElementField f : field) {
                for (ValueObject val : f.getValues(tn, state)) {
                    if (val == null && !emptyok()) {
                        fvlist.rollback();
                        return 0;
                    }
                    if (f.show()) {
                        fvlist.push(f.field(fvlist.getFormat()), val);
                        updates++;
                    }
                }
            }
        }
        fvlist.commit();
        return updates;
    }
}
