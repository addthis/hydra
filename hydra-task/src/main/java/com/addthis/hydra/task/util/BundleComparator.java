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
package com.addthis.hydra.task.util;

import java.lang.Override;import java.lang.String;import java.util.Comparator;

import com.addthis.bundle.core.Bundle;import com.addthis.bundle.core.BundleField;import com.addthis.bundle.core.BundleFormat;import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * Specifies one or more fields of the bundle that will be used in sorting.
 * <p>If the parameter 'ascending' is not specified, then all fields are sorted in ascending order.
 *
 * @user-reference
 */
public class BundleComparator implements Comparator<Bundle> {

    /**
     * A sequence of field names that will be used in sorting. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String[] field;

    /**
     * If specified, then a sequence of boolean values that determine if fields are sorted in
     * ascending order.
     * This parameter must be of the same length as the 'field' parameter.
     */
    @FieldConfig(codable = true)
    private boolean[] ascending;

    @FieldConfig(codable = true)
    private int defaultValue = -1;

    @Override
    public int compare(final Bundle left, final Bundle right) {
        if (left == null || right == null) {
            return defaultValue;
        }
        final BundleFormat formatLeft = left.getFormat();
        final BundleFormat formatRight = right.getFormat();
        BundleField[] fieldLeft = new BundleField[field.length];
        for (int i = 0; i < field.length; i++) {
            fieldLeft[i] = formatLeft.getField(field[i]);
        }
        BundleField[] fieldRight;
        if (formatRight == formatLeft) {
            fieldRight = fieldLeft;
        } else {
            fieldRight = new BundleField[field.length];
            for (int i = 0; i < field.length; i++) {
                fieldRight[i] = formatRight.getField(field[i]);
            }
        }
        for (int i = 0; i < field.length; i++) {
            final ValueObject vLeft = left.getValue(fieldLeft[i]);
            final ValueObject vRight = right.getValue(fieldRight[i]);
            boolean asc = ascending == null || ascending[i];
            if (vLeft == vRight) {
                continue;
            }
            if (vLeft == null) {
                return asc ? 1 : -1;
            }
            if (vRight == null) {
                return asc ? -1 : 1;
            }
            final ValueObject.TYPE tLeft = vLeft.getObjectType();
            final ValueObject.TYPE tRight = vRight.getObjectType();
            if (tLeft != tRight || tLeft == ValueObject.TYPE.STRING) {
                final int val = vLeft.toString().compareTo(vRight.toString());
                if (val != 0) {
                    return asc ? val : -val;
                } else {
                    continue;
                }
            }
            int val = defaultValue;
            switch (tLeft) {
                case FLOAT:
                    final double vl = vLeft.asDouble().getDouble();
                    final double vr = vRight.asDouble().getDouble();
                    if (vl != vr) {
                        val = vl > vr ? 1 : -1;
                    }
                    break;
                case INT:
                    final long il = vLeft.asLong().getLong();
                    final long ir = vRight.asLong().getLong();
                    if (il != ir) {
                        val = il > ir ? 1 : -1;
                    }
                    break;
            }
            if (val != 0) {
                return asc ? val : -val;
            }
        }
        return defaultValue;
    }
}
