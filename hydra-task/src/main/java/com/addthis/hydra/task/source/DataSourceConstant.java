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
package com.addthis.hydra.task.source;

import javax.annotation.Nullable;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This data source <span class="hydra-summary">creates specified constant bundles</span>.
 * <p>
 * The repeat parameter determines how many bundles are created. The default
 * value of maxPackets (-1) indicates that an infinite number of bundles are created.
 * <p/>
 *
 * @user-reference
 * @hydra-name const
 */
public class DataSourceConstant extends TaskDataSource {

    /** The constant bundles. Required */
    @JsonProperty(required = true)
    private Bundle[] bundles;

    /**
     * How many times to repeat the bundles. Optional. Default is 0.
     *
     * 0 to send the constant bundles once. Any negative value to repeatedly send the bundles
     * indefinitely.
     */
    @JsonProperty
    private long repeat = 0;

    private boolean closed = false;
    private int cursor = 0;

    @Override public void init() {
    }

    @Nullable
    @Override
    public Bundle peek() {
        return closed ? null : currentBundle();
    }

    @Override public Bundle next() {
        Bundle val = peek();
        advanaceCursor();
        if (val != null) {
            val = Bundles.deepCopyBundle(val);
        }
        return val;
    }

    private Bundle currentBundle() {
        return (cursor >= bundles.length || cursor < 0)? null : bundles[cursor];
    }

    private void advanaceCursor() {
        if (cursor < bundles.length) {
            cursor++;
            if (cursor == bundles.length) {
                if (repeat < 0) {
                    cursor = 0;
                } else if (repeat > 0){
                    repeat--;
                    cursor = 0;
                }
            }
        }
    }

    @Override public void close() {
        closed = true;
    }
}
