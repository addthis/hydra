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

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.tree.prop.DataSeen;
import com.addthis.hydra.store.util.SeenFilterBasic;


/**
 * <p>This query operation <span class="hydra-summary">applies a bloom filter to a column</span>.
 * <p/>
 * <p>The syntax for this operation is seen=column:bits:bitsper:hash. 'column' is the column
 * number. 'bits' is the cardinality of the bloom filter (total number of bits allocated
 * to the filter). It must be 32 or greater. 'bitsper' is the number of hash function
 * evaluations for each insertion operation. This parameter is usually referred to as
 * the "k" parameter in the literature. 'hash' is the type of hash function to apply.
 * The types of hash functions are listed in {@link SeenFilterBasic#hash hash}.
 *
 * @user-reference
 * @hydra-name seen
 */
public class OpSeen extends AbstractTableOp {

    /**
     * Columns to summarize
     */
    private int column;
    private int bits;
    private int bitsper; // defaults to 4
    private int hash; // defaults to 4

    public OpSeen(DataTableFactory tableFactory, String args) {
        super(tableFactory);
        String v[] = Strings.splitArray(args, ":");
        this.column = Integer.parseInt(v[0]);
        this.bits = Integer.parseInt(v[1]);
        this.bitsper = v.length > 2 ? Integer.parseInt(v[2]) : 4;
        this.hash = v.length > 3 ? Integer.parseInt(v[3]) : 4;
    }

    @Override
    public DataTable tableOp(DataTable result) {
        if (column >= result.size()) {
            return result;
        }
        SeenFilterBasic<String> seen = new SeenFilterBasic<>(bits, bitsper, hash);
        ValueObject obj = new DataSeen.ValueBloom(seen);
        BundleColumnBinder binder = getSourceColumnBinder(result);
        for (Bundle row : result) {
            seen.setSeen(binder.getColumn(row, column).toString());
        }
        DataTable ret = createTable(1);
        Bundle row = ret.createBundle();
        binder.appendColumn(row, obj);
        ret.append(row);
        return ret;
    }

}
