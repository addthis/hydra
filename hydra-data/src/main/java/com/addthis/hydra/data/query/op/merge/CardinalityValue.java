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

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.op.MergedRow;
import com.addthis.hydra.data.tree.prop.DataCounting;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CardinalityValue extends AbstractMergedValue<ValueObject> {
    private static final Logger log = LoggerFactory.getLogger(CardinalityValue.class);

    @Override protected ValueObject doMerge(ValueObject nextValue, ValueObject value) {
        DataCounting.LCValue lcValue = toLcValue(value);
        ICardinality estimator = lcValue.asNative();
        updateCounter(estimator, nextValue);
        return lcValue;
    }

    @Override protected ValueObject convert(ValueObject nextValue) {
        return nextValue;
    }

    @Override protected ValueObject doEmit(ValueObject value, MergedRow mergedRow) {
        return toLcValue(value);
    }

    private DataCounting.LCValue toLcValue(ValueObject value) {
        if (value instanceof DataCounting.LCValue) {
            return (DataCounting.LCValue) value;
        } else {
            ICardinality estimator = new HyperLogLogPlus(14, 25);
            updateCounter(estimator, value);
            return new DataCounting.LCValue(estimator);
        }
    }

    private static void updateCounter(ICardinality estimator, ValueObject value) {
        if (value == null) {
            return;
        }
        switch (value.getObjectType()) {
            case INT:
            case FLOAT:
            case STRING:
            case BYTES:
            case CUSTOM:
                estimator.offer(value.toString());
                break;
            case ARRAY:
                ValueArray arr = value.asArray();
                for (ValueObject o : arr) {
                    updateCounter(estimator, o);
                }
                break;
            case MAP:
                ValueMap map = value.asMap();
                for (ValueMapEntry o : map) {
                    updateCounter(estimator, ValueFactory.create(o.getKey()));
                }
                break;
        }
    }

}
