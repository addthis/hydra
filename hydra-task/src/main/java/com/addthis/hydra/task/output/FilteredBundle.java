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
package com.addthis.hydra.task.output;

import java.util.HashSet;
import java.util.Iterator;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleException;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.value.ValueObject;

public class FilteredBundle implements Bundle {

    private final Bundle wrap;
    private final BundleFormat wrapFormat;

    FilteredBundle(Bundle wrap, HashSet<String> include, HashSet<String> exclude) {
        this.wrap = wrap;
        this.wrapFormat = new FilteredFormat(wrap.getFormat(), include, exclude);
    }

    @Override
    public Iterator<BundleField> iterator() {
        return wrapFormat.iterator();
    }

    @Override
    public ValueObject getValue(BundleField field) throws BundleException {
        return wrap.getValue(field);
    }

    @Override
    public void setValue(BundleField field, ValueObject value) throws BundleException {
        wrap.setValue(field, value);
    }

    @Override
    public void removeValue(BundleField field) throws BundleException {
        wrap.removeValue(field);
    }

    @Override
    public BundleFormat getFormat() {
        return wrapFormat;
    }

    @Override
    public int getCount() {
        return wrap.getCount();
    }

    @Override
    public Bundle createBundle() {
        return new FilteredBundle(wrap.createBundle(), null, null);
    }
}
