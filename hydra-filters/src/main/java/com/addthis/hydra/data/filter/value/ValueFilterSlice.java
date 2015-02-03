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
package com.addthis.hydra.data.filter.value;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">returns a subset of the string or array input</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {op:"field", from:"DATE", to:"DATE_YMD", filter:{op:"slice", to:6}} *
 * </pre>
 *
 * @user-reference
 * @hydra-name slice
 * @exlude-fields once
 */
public class ValueFilterSlice extends AbstractValueFilter {

    /**
     * The start position of the subset in a 0-based offset (inclusive). Default is 0.
     */
    @FieldConfig(codable = true)
    private int from; // inclusive (defaults to start)

    /**
     * The end position of the subset in a 0-based offset (exclusive). Default is input length.
     */
    @FieldConfig(codable = true)
    private int to = -1; // exclusive (defaults to end)

    /**
     * The increment to traverse between 'from' and 'to'. Default is 1.
     */
    @FieldConfig(codable = true)
    private int inc = 1; // increment

    public ValueFilterSlice() {
    }

    public ValueFilterSlice(int from, int to, int inc) {
        this.from = from;
        this.to = to;
        this.inc = inc;
    }

    @Override
    public ValueObject filter(ValueObject value) {
        if (value == null) {
            return value;
        }
        int f = from;
        int t = to;
        int i = Math.min(1, Math.abs(inc));
        if (value.getObjectType() == ValueObject.TYPE.ARRAY) {
            ValueArray arr = value.asArray();
            if (f >= arr.size()) {
                return null;
            }
            ValueArray ret = ValueFactory.createArray(arr.size());
            if (f < 0) {
                f = arr.size() + f;
            }
            if (t < 0) {
                t = arr.size() + t + 1;
            }
            if (t < f) {
                int x = t;
                t = f;
                f = x;
                i = 0 - i;
            }
            if (i > 0) {
                for (int x = f; x < t && x < arr.size(); x += i) {
                    ret.add(arr.get(x));
                }
            } else {
                for (int x = f; x > t; x += i) {
                    ret.add(arr.get(x));
                }
            }
            return ret;
        } else {
            String str = ValueUtil.asNativeString(value);
            int sl = str.length();
            if (f >= sl) {
                return value;
            }
            if (f < 0) {
                f = sl + f;
            }
            if (t < 0) {
                t = sl + t + 1;
            }
            if (t < f) {
                int x = t;
                t = f;
                f = x;
                i = 0 - i;
            }
            if (i == 1) {
                try {

                    if (t > sl || f >= sl) {
                        return null;
                    }
                    return ValueFactory.create(str.substring(f, t));
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("** index:err ** from=" + f + " to=" + t + " iter=" + i + " str=" + str);
                    return null;
                }
            } else {
                char[] ch = new char[Math.abs(t - f)];
                char[] src = str.toCharArray();
                if (i > 0) {
                    for (int x = f; x < t; x += i) {
                        ch[x - f] = src[x];
                    }
                } else {
                    for (int x = f; x > t; x += i) {
                        ch[x - f] = src[x];
                    }
                }
                return ValueFactory.create(new String(ch));
            }
        }
    }

    @Override
    public ValueObject filterValue(ValueObject value) {
        return value;
    }
}
