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
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractRowOp;

import io.netty.channel.ChannelProgressivePromise;


/**
 * fill: create empty columns up to the length of the bundleformat
 * pad: expand all rows to a fixed number of columns
 */
public class OpFill extends AbstractRowOp {

    private ValueObject fillWith;
    private Integer pad = null;
    private String[] fields;

    public OpFill(String args, ChannelProgressivePromise queryPromise) {
        super(queryPromise);
        if (args != null && args.length() > 0) {
            switch (args.charAt(0)) {
                case 'i':
                case 'l':
                    fillWith = ValueFactory.create(Long.parseLong(args.substring(1)));
                    break;
                case 'f':
                case 'd':
                    fillWith = ValueFactory.create(Double.parseDouble(args.substring(1)));
                    break;
                case 's':
                    fillWith = ValueFactory.create(args.substring(1));
                    break;
                default:
                    fillWith = ValueFactory.create(Long.parseLong(args));
                    break;
            }
        } else {
            fillWith = ValueFactory.create(0);
        }
    }

    public OpFill(String args, boolean pad, ChannelProgressivePromise queryPromise) {
        this(Strings.splitArray(args, ":")[0], queryPromise);
        if (pad) {
            String[] pair = Strings.splitArray(args, ":");

            this.pad = Integer.valueOf(pair[1]);
            this.fields = new String[this.pad];
            for (int i = 0; i < this.pad; i++) {
                fields[i] = String.valueOf(i);
            }
        }
    }


    @Override
    public Bundle rowOp(Bundle line) {
        if (pad == null) {
            for (BundleField field : line.getFormat()) {
                if (line.getValue(field) == null) {
                    line.setValue(field, fillWith);
                }
            }
        } else {
            BundleColumnBinder binder = getSourceColumnBinder(line, fields);
            for (int i = 0; i < fields.length; i++) {
                if (binder.getColumn(line, Integer.valueOf(fields[i])) == null) {
                    binder.setColumn(line, Integer.valueOf(fields[i]), fillWith);
                }

            }
        }
        return line;
    }
}
