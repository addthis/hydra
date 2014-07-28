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

import java.util.StringTokenizer;

import com.addthis.basis.util.Bytes;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;

import org.apache.commons.lang3.mutable.MutableInt;

public class BoundedValue implements SuperCodable {

    // TODO resolve - using Integers b/c JSON objects turn all nums into
    // ints
    @FieldConfig(codable = true)
    public String name;
    @FieldConfig(codable = true)
    public Long gt;
    @FieldConfig(codable = true)
    public Long lt;
    @FieldConfig(codable = true)
    public Long eq;
    @FieldConfig(codable = false)
    public boolean bounded;

    public BoundedValue parse(String tok, MutableInt nextColumn) {
        StringTokenizer st = new StringTokenizer(Bytes.urldecode(tok), "<>=", true);
        name = st.nextToken();
        while (st.hasMoreTokens()) {
            String next = st.nextToken();
            if (next.equals(">") && st.hasMoreTokens()) {
                gt = Long.parseLong(st.nextToken());
                bounded = true;
            }
            if (next.equals("<") && st.hasMoreTokens()) {
                lt = Long.parseLong(st.nextToken());
                bounded = true;
            }
            if (next.equals("=") && st.hasMoreTokens()) {
                eq = Long.parseLong(st.nextToken());
                bounded = true;
            }
        }
        return this;
    }

    void toCompact(StringBuilder sb) {
        sb.append(Bytes.urlencode(name != null ? name : ""));
        if (gt != null) {
            sb.append(">" + gt);
        }
        if (lt != null) {
            sb.append("<" + lt);
        }
        if (eq != null) {
            sb.append("=" + eq);
        }
    }

    public boolean validate(String sval) {
        return bounded ? validate(Long.parseLong(sval)) : true;
    }

    public boolean validate(long value) {
        if (bounded) {
            if (eq != null && value == eq) {
                return true;
            }
            if (gt != null && value > gt) {
                return true;
            }
            if (lt != null && value < lt) {
                return true;
            }
            return false;
        }
        return true;
    }

    @Override
    public void postDecode() {
        bounded = gt != null || lt != null || eq != null;
    }

    @Override
    public void preEncode() {
    }
}
