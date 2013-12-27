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

import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.codec.Codec.ClassMap;
import com.addthis.codec.Codec.ClassMapFactory;
import com.addthis.hydra.common.plugins.PluginReader;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

/**
 * A value filter applies a transformation on a value and returns
 * the result of the transformation.
 *
 * @user-reference
 * @hydra-category
 * @exclude-fields once, nullAccept
 */
@Codec.Set(classMapFactory = ValueFilter.CMAP.class)
public abstract class ValueFilter implements Codec.Codable {

    public static final ClassMap cmap = new ClassMap() {
        @Override
        public String getClassField() {
            return "op";
        }

        @Override
        public String getCategory() {
            return "value filter";
        }

        @Override
        public ClassMap misnomerMap() {
            return BundleFilter.cmap;
        }
    };

    /**
     * @exclude
     */
    public static class CMAP implements ClassMapFactory {

        public ClassMap getClassMap() {
            return cmap;
        }
    }

    /** register types */
    static {
        PluginReader.registerPlugin("-valuefilters.classmap", cmap, ValueFilter.class);
    }

    /**
     * If true and input is array then apply filter once on input.
     * Otherwise apply filter to each element of the array.
     * Default is false.
     */
    @Codec.Set(codable = true)
    private boolean once;
    /**
     * If true then a parent {@link ValueFilterChain chain} filter does not exit on null values.
     * This indicates that the filter wishes to accept a null
     * returned by the previous filter in the chain. Default is false.
     *
     * @return
     */
    @Codec.Set(codable = true)
    private boolean nullAccept;

    /**
     * overrides default chain behavior to exit on null values.
     * this indicates that the filter wishes to accept a null
     * returned by a previous filter in the chain.
     *
     * @return
     */
    public boolean nullAccept() {
        return nullAccept;
    }

    public boolean getOnce() {
        return once;
    }

    public ValueFilter setOnce(boolean o) {
        once = o;
        return this;
    }

    public ValueFilter setNullAccept(boolean na) {
        nullAccept = na;
        return this;
    }

    private ValueObject filterArray(ValueObject value) {
        ValueArray in = value.asArray();
        ValueArray out = null;
        for (ValueObject vo : in) {
            ValueObject val = filterValue(vo);
            if (val != null) {
                if (out == null) {
                    out = ValueFactory.createArray(in.size());
                }
                out.append(val);
            }
        }
        return out;
    }

    /**
     * handles array iteration
     */
    public ValueObject filter(ValueObject value) {
        if (once) {
            return filterValue(value);
        }
        // TODO why is this behaviour not there for TYPE.MAPS ?
        if (value != null && value.getObjectType() == ValueObject.TYPE.ARRAY) {
            return filterArray(value);
        }
        return filterValue(value);
    }

    /**
     * override in subclasses
     */
    public abstract ValueObject filterValue(ValueObject value);

    private AtomicBoolean setup = new AtomicBoolean(false);
    private AtomicBoolean setupOnce = new AtomicBoolean(false);
    private Object setupLock = new Object();

    /**
     * ensures setup() is called exactly once and that all other
     * threads block on filter until this is done.  attempts to
     * be efficient over time by avoiding sync calls on each filter call.
     */
    public final void requireSetup() {
        if (!setup.get()) {
            synchronized (setupLock) {
                if (setupOnce.compareAndSet(false, true)) {
                    setup();
                    setup.set(true);
                }
            }
        }
    }

    public void setup() {
        // TODO override in subclasses that need atomic setup
    }
}
