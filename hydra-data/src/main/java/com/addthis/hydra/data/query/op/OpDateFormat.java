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
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.util.ValueUtil;
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
 * datef=incols:informat:outformat:outcol
 * datef=0:yyMMdd:yymm would transform (090909 to 0909)
 * </pre>
 * <p><b>incols</b> is optionally a comma separated list of columns to be concatenated together.</p>
 * <p>if <b>outcol<b> is not set, the first column of <b>incols</b> is used </p>
 * <p></p><b>informat</b> and/or <b>outformat</b> can optionally be <i>unixmillis</i> to convert to or from a base10 unix millis epoch offset value.</p>
 *
 * @user-reference
 * @hydra-name datef
 */
public class OpDateFormat extends AbstractRowOp {

    private int incols[];
    private int outcol;

    private DateTimeFormatter inFormat;
    private DateTimeFormatter outFormat;
    private int fromMillis; // base for conversion or 0
    private int toMillis; // base for conversion or 0

    public OpDateFormat(String args) {
        try {
            String opt[] = args.split(":");
            if (opt.length >= 2) {
                String cval[] = Strings.splitArray(opt[0], ",");
                incols = new int[cval.length];
                for (int i = 0; i < cval.length; i++) {
                    incols[i] = Integer.parseInt(cval[i]);
                }
                outcol = opt.length > 2 ? Integer.parseInt(opt[3]) : incols[0];
                if ((fromMillis = parseMillis(opt[1])) == 0) {
                    inFormat = DateTimeFormat.forPattern(opt[1]);
                }
                if ((toMillis = parseMillis(opt[2])) == 0) {
                    outFormat = DateTimeFormat.forPattern(opt[2]);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * TODO future support format "unixmillis:base"
     * but for now always use base 10
     */
    private int parseMillis(String value) {
        if (value.startsWith("unixmillis")) {
            return 10;
        } else {
            return 0;
        }
    }

    @Override
    public Bundle rowOp(Bundle row) {
        BundleColumnBinder binder = getSourceColumnBinder(row);
        // concatenate columns
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < incols.length; i++) {
            String s = ValueUtil.asNativeString(binder.getColumn(row, incols[i]));
            if (s != null) {
                buf.append(s);
            }
        }
        // convert input
        String sbuf = buf.toString();
        DateTime dt = null;
        if (fromMillis > 0) {
            dt = new DateTime(Integer.parseInt(sbuf, fromMillis));
        } else {
            dt = inFormat.parseDateTime(sbuf);
        }
        // convert and output
        ValueObject out = null;
        if (toMillis > 0) {
            out = ValueFactory.create(dt.getMillis());
        } else {
            out = ValueFactory.create(outFormat.print(dt));
        }
        if (outcol >= row.getCount()) {
            binder.appendColumn(row, out);
        } else {
            binder.setColumn(row, outcol, out);
        }
        return row;
    }
}
