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
package com.addthis.hydra.data.filter.value;

import javax.annotation.Nullable;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A value filter applies a transformation on a value and returns
 * the result of the transformation.
 *
 * @user-reference
 * @hydra-category
 * @exclude-fields once, nullAccept
 */
@Pluggable("value-filter")
public abstract class ValueFilter implements Codable {

    /**
     * Disables special {@link ValueArray} handling logic. By default (false), it will map over elements
     * of an array. When this flag is turned on, filters will try to process the array as a whole.
     * Default is false.
     */
    @JsonProperty private boolean once;

    /**
     * If true then a parent {@link ValueFilterChain chain} filter does not exit on null values.
     * This indicates that the filter wishes to accept a null
     * returned by the previous filter in the chain. Default is false.
     */
    @JsonProperty private boolean nullAccept;

    // used for setup and requireSetup to do one-time-only initialization logic
    private final Lock setupLock = new ReentrantLock();
    private boolean setup = false;

    public boolean getNullAccept() {
        return nullAccept;
    }

    public boolean getOnce() {
        return once;
    }

    @Deprecated
    public ValueFilter setOnce(boolean o) {
        once = o;
        return this;
    }

    @Nullable private ValueObject filterArray(ValueObject value) {
        ValueArray in = value.asArray();
        ValueArray out = null;
        for (ValueObject vo : in) {
            ValueObject val = filterValue(vo);
            if (val != null) {
                if (out == null) {
                    out = ValueFactory.createArray(in.size());
                }
                out.add(val);
            }
        }
        return out;
    }

    /**
     * Wrapper method for {@link #filterValue(ValueObject)} that has special logic for {@link ValueArray}s.
     * This should be the primary method to be called in most circumstances, and should not be overridden unless
     * special, different array handling logic is needed. When {@link #once} is true, or when the ValueObject
     * is not an array, this is the same as directly calling {@link #filterValue(ValueObject)}.
     */
    @Nullable public ValueObject filter(@Nullable ValueObject value) {
        if (once) {
            return filterValue(value);
        }
        // TODO why is this behaviour not there for TYPE.MAPS ?
        if ((value != null) && (value.getObjectType() == ValueObject.TYPE.ARRAY)) {
            return filterArray(value);
        }
        return filterValue(value);
    }

    @Nullable public abstract ValueObject filterValue(@Nullable ValueObject value);

    /**
     * ensures setup() is called exactly once and that all other
     * threads block on filter until this is done.  attempts to
     * be efficient over time by avoiding sync calls on each filter call.
     */
    public final void requireSetup() {
        if (!setup) {
            setupLock.lock();
            try {
                if (!setup) {
                    setup();
                    setup = true;
                }
            } finally {
                setupLock.unlock();
            }
        }
    }

    // override in subclasses that need atomic setup
    public void setup() {}
}
