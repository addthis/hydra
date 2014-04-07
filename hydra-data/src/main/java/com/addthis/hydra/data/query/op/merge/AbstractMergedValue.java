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

import javax.annotation.Nullable;

import com.addthis.bundle.value.ValueObject;

public abstract class AbstractMergedValue<T extends ValueObject> implements MergedValue {

    protected T value = null;
    private final boolean acceptNull;

    public AbstractMergedValue(boolean acceptNull) {
        this.acceptNull = acceptNull;
    }

    public AbstractMergedValue() {
        this(false);
    }

    @Override @Nullable
    public ValueObject emit() {
        if (value == null) {
            return null;
        } else {
            return doEmit();
        }
    }

    @Override
    public void merge(ValueObject nextValue) {
        if ((nextValue != null) || acceptNull) {
            if (value == null) {
                value = convert(nextValue);
            } else {
                doMerge(convert(nextValue));
            }
        }
    }

    @Override
    public boolean isKey() {
        return false;
    }

    protected abstract void doMerge(T nextValue);

    protected abstract T convert(ValueObject nextValue);

    protected T doEmit() {
        return value;
    }
}
