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

import com.addthis.bundle.value.ValueNumber;

public class AverageValue extends SumValue {

    private int count;

    public AverageValue(int count) {
        this.count = count;
    }

    public AverageValue() {
        this(1); // the first value does not hit doEmit
    }

    @Override
    public ValueNumber doEmit() {
        return value.avg(count);
    }

    @Override
    public void doMerge(ValueNumber nextValue) {
        super.doMerge(nextValue);
        count += 1;
    }
}
