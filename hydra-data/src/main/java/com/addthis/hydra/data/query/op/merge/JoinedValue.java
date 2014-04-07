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

package com.addthis.hydra.data.query.op.merge;

import java.util.ArrayList;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueString;

import com.google.common.base.Joiner;

public class JoinedValue extends AbstractMergedValue<ValueString> {

    private static final Joiner DEFAULT_JOINER = Joiner.on(',');

    private final Joiner joiner;
    private final ArrayList<String> values;

    public JoinedValue(Joiner joiner) {
        this.joiner = joiner;
        this.values = new ArrayList<>(5);
    }

    public JoinedValue(String seperator) {
        this(Joiner.on(seperator));
    }

    public JoinedValue() {
        this(DEFAULT_JOINER);
    }

    @Override
    protected void doMerge(ValueString nextValue) {
        if (values.isEmpty()) {
            values.add(value.toString());
        }
        values.add(nextValue.toString());
    }

    @Override
    protected ValueString convert(ValueObject nextValue) {
        return nextValue.asString();
    }

    @Override
    protected ValueString doEmit() {
        if (values.isEmpty()) {
            return value;
        } else {
            return ValueFactory.create(joiner.join(values));
        }
    }
}
