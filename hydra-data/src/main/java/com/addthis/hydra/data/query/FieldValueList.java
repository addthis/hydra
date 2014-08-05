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
package com.addthis.hydra.data.query;

import java.util.LinkedList;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFactory;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.BundleFormatted;
import com.addthis.bundle.value.ValueObject;


public class FieldValueList implements BundleFormatted {

    private final LinkedList<FieldValue> list = new LinkedList<>();
    private final BundleFormat bundleFormat;
    private int lastCommit;

    public FieldValueList(BundleFormat bundleFormat) {
        this.bundleFormat = bundleFormat;
    }

    /**
     * pushes must be committed
     */
    public void push(FieldValue fv) {
        list.addLast(fv);
    }

    /**
     * pushes must be committed
     */
    public void push(BundleField field, ValueObject value) {
        push(new FieldValue(field, value));
    }

    /**
     * pops are immediate and must NOT be committed
     */
    public void pop() {
        list.removeLast();
        lastCommit = list.size();
    }

    public void pop(int count) {
        while (count-- > 0) {
            pop();
        }
    }

    public void commit() {
        lastCommit = list.size();
    }

    public void rollback() {
        while (list.size() > lastCommit) {
            list.removeLast();
        }
    }

    public boolean updateBundle(Bundle bundle) {
        for (FieldValue fv : list) {
            bundle.setValue(fv.field, fv.value);
        }
        return list.size() > 0;
    }

    public Bundle createBundle(BundleFactory factory) {
        Bundle bundle = factory.createBundle();
        for (FieldValue fv : list) {
            bundle.setValue(bundle.getFormat().getField(fv.field.getName()), fv.value);
        }
        return bundle;
    }

    @Override public BundleFormat getFormat() {
        return bundleFormat;
    }
}
