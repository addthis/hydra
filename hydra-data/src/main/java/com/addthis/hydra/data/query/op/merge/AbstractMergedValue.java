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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.op.MergedRow;

public abstract class AbstractMergedValue<T extends ValueObject> implements MergedValue {

    protected BundleField from;
    protected BundleField to;

    @Override
    public void emit(MergedRow mergedRow) {
        ValueObject value = mergedRow.getValue(to);
        if (value != null) {
            value = doEmit(convert(value), mergedRow);
            mergedRow.setValue(to, value);
        }
    }

    @Override
    public void merge(Bundle nextBundle, MergedRow mergedRow) {
        ValueObject nextValue = nextBundle.getValue(from);
        if (nextValue == null) {
            return;
        }
        ValueObject value = mergedRow.getValue(to);
        ValueObject mergedValue = merge(nextValue, value);
        mergedRow.setValue(to, mergedValue);
    }

    protected ValueObject merge(ValueObject nextValue, ValueObject value) {
        T nextValueT = convert(nextValue);
        if (value == null) {
            return nextValueT;
        } else {
            return doMerge(nextValueT, convert(value));
        }
    }

    @Override
    public boolean isKey() {
        return false;
    }

    protected abstract T doMerge(T nextValue, T value);

    protected abstract T convert(ValueObject<?> nextValue);

    protected T doEmit(T value, MergedRow mergedRow) {
        return value;
    }

    @Override
    public BundleField getFrom() {
        return from;
    }

    @Override
    public void setFrom(BundleField from) {
        this.from = from;
    }

    @Override
    public BundleField getTo() {
        return to;
    }

    @Override
    public void setTo(BundleField to) {
        this.to = to;
    }
}
