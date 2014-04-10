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

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.op.MergedRow;

import com.google.common.base.Joiner;

public class JoinedValue extends AbstractMergedValue<ValueObject> {

    private static final Joiner DEFAULT_JOINER = Joiner.on(',');

    private final Joiner joiner;

    public JoinedValue(Joiner joiner) {
        this.joiner = joiner;
    }

    public JoinedValue(String seperator) {
        this(Joiner.on(seperator));
    }

    public JoinedValue() {
        this(DEFAULT_JOINER);
    }

    @Override
    protected ValueObject doMerge(ValueObject nextValue, ValueObject value) {
        ValueArray values = ValueUtil.asArray(value);
        values.add(nextValue);
        return values;
    }

    @Override
    protected ValueObject convert(ValueObject nextValue) {
        if (nextValue.getObjectType() != ValueObject.TYPE.ARRAY) {
            return nextValue.asString();
        } else {
            return nextValue;
        }
    }

    @Override
    protected ValueObject doEmit(ValueObject value, MergedRow mergedRow) {
        ValueArray values = ValueUtil.asArray(value);
        return ValueFactory.create(joiner.join(values));
    }
}
