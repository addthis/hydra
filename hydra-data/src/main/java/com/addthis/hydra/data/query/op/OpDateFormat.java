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


import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractRowOp;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * <p>This query operation <span class="hydra-summary">transforms from one date format to another</span>.
 * <p/>
 * <p>The input and output date formats are specified using the <a href="http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html">DateTimeFormat</a></p>
 * <p/>
 * <pre>
 * datef=col:informat:outformat
 * datef=0:yyMMdd:yymm would transform (090909 to 0909)
 * </pre>
 *
 * @user-reference
 * @hydra-name datef
 */
public class OpDateFormat extends AbstractRowOp {

    private int col;

    private DateTimeFormatter inFormat;
    private DateTimeFormatter outFormat;

    public OpDateFormat(String args) {
        try {
            String opt[] = args.split(":");
            if (opt.length >= 2) {
                col = Integer.parseInt(opt[0]);
                inFormat = DateTimeFormat.forPattern(opt[1]);
                outFormat = DateTimeFormat.forPattern(opt[2]);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        if (col < row.getCount()) {
            BundleColumnBinder binder = getSourceColumnBinder(row);
            ValueObject oldval = binder.getColumn(row, col);
            if (oldval != null) {
                DateTime dt = inFormat.parseDateTime(oldval.toString());
                String newval = outFormat.print(dt);
                binder.setColumn(row, col, ValueFactory.create(newval));
            }
        }
        return row;
    }
}
