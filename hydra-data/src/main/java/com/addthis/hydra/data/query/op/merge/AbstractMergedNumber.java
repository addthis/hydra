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
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractQueryOp;

public abstract class AbstractMergedNumber extends AbstractMergedValue<ValueNumber> {

    public AbstractMergedNumber(boolean acceptNull) {
        super(acceptNull);
    }

    public AbstractMergedNumber() {
        super();
    }

    @Override
    protected ValueNumber convert(ValueObject nextValue) {
        ValueNumber num = ValueUtil.asNumberOrParseLong(nextValue, 10);
        return (num != null) ? num : AbstractQueryOp.ZERO;
    }
}
