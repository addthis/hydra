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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractTableOp;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * <p>This query operation <span class="hydra-summary">removes (key, value) pairs
 * that are observed once</span>.
 * <p/>
 * <p>The syntax for this operation is rmsing=N:M where N is the column number for
 * the key column and M is the column number for the value column. Each (key, value)
 * pair that exists only once in the output rows is removed.
 *
 * @user-reference
 * @hydra-name rmsing
 */
public class OpRemoveSingletons extends AbstractTableOp {

    private Logger log = LoggerFactory.getLogger(OpChangePoints.class);
    int keyColumn;
    int valColumn;

    public OpRemoveSingletons(DataTableFactory factory, String args) {
        super(factory);
        try {
            String[] opt = args.split(":");
            keyColumn = opt.length >= 1 ? Integer.parseInt(opt[0]) : 0;
            valColumn = opt.length >= 2 ? Integer.parseInt(opt[1]) : 1;
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }

    /**
     * Strip out rows with keys that map to a single value
     *
     * @param result Input data table
     * @return Final data table
     */
    @Override
    public DataTable tableOp(final DataTable result) {
        if (result == null || result.size() == 0) {
            return result;
        }
        Long[] data = new Long[result.size()];
        BundleField[] fields = new BundleColumnBinder(result.get(0)).getFields();
        BundleField keyField = fields[keyColumn];
        BundleField valField = fields[valColumn];
        ValueObject oldKey = null;
        Set<String> vals = new HashSet<>();
        List<Bundle> rows = new ArrayList<>();
        DataTable table = createTable(0);
        for (int i = 0; i < result.size(); i++) {
            Bundle row = result.get(i);
            ValueObject newKey = row.getValue(keyField);
            if (oldKey == null) {
                oldKey = newKey;
            } else if ((!oldKey.equals(newKey)) || i == result.size() - 1) {
                if (i == result.size() - 1) {
                    rows.add(row);
                }
                if (vals.size() > 1) {
                    for (Bundle storedRow : rows) {
                        table.append(storedRow);
                    }
                }
                oldKey = newKey;
                vals.clear();
                rows.clear();
            }
            rows.add(row);
            String newVal = row.getValue(valField).asString().toString();
            vals.add(newVal);
        }
        return table;
    }
}
