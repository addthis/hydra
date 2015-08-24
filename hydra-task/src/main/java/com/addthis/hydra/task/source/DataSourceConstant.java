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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This data source <span class="hydra-summary">creates specified constant bundles</span>.
 *
 * @user-reference
 */
public class DataSourceConstant extends TaskDataSource {

    /** 0 or more constant bundles. Required */
    @JsonProperty(required = true)
    private List<Bundle> bundles;

    /**
     * How many times to repeat the bundles. Optional. Default is 0.
     * <p/>
     * 0 to send the constant bundles once. Any negative value to repeatedly send the bundles
     * indefinitely.
     */
    @JsonProperty
    private int repeat = 0;

    private PeekingIterator<Bundle> iterator;
    private boolean closed = false;

    @Override public void init() {
        Iterator<Bundle> iter;
        if (repeat == 0) {
            iter = bundles.iterator();
        } else {
            iter = Iterators.cycle(bundles);
            if (repeat > 0) {
                iter = Iterators.limit(iter, (repeat + 1) * bundles.size());
            }
        }
        iterator = Iterators.peekingIterator(iter);
    }

    @Nullable @Override public Bundle peek() {
        checkNotClosed();
        return iterator.peek();
    }

    @Nullable @Override public Bundle next() {
        checkNotClosed();
        return Bundles.deepCopyBundle(iterator.next());
    }

    private void checkNotClosed() {
        if (closed) {
            throw new NoSuchElementException();
        }
    }

    @Override public void close() {
        closed = true;
    }
}
