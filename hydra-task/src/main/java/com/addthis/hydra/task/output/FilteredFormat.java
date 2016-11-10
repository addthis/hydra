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
import java.util.NoSuchElementException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;

public class FilteredFormat implements BundleFormat {

    private final BundleFormat wrap;
    private final HashSet<String> include;
    private final HashSet<String> exclude;

    FilteredFormat(BundleFormat wrap, HashSet<String> include, HashSet<String> exclude) {
        this.wrap = wrap;
        this.include = include;
        this.exclude = exclude;
    }

    @Override
    public Object getVersion() {
        return wrap.getVersion();
    }

    @Override
    public Iterator<BundleField> iterator() {
        return new Iterator<BundleField>() {
            final Iterator<BundleField> iter = wrap.iterator();
            BundleField next;

            @Override
            public boolean hasNext() {
                while (next == null && iter.hasNext()) {
                    BundleField peek = iter.next();
                    if (include != null) {
                        if (include.contains(peek.getName())) {
                            next = peek;
                        }
                    } else if (exclude != null) {
                        if (!exclude.contains(peek.getName())) {
                            next = peek;
                        }
                    } else {
                        next = peek;
                    }
                }
                return next != null;
            }

            @Override
            public BundleField next() {
                if (hasNext()) {
                    BundleField ret = next;
                    next = null;
                    return ret;
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public BundleField getField(String name) {
        return wrap.getField(name);
    }

    @Override
    public boolean hasField(String name) {
        return wrap.hasField(name);
    }

    @Override
    public BundleField getField(int pos) {
        return wrap.getField(pos);
    }

    @Override
    public int getFieldCount() {
        return wrap.getFieldCount();
    }

    @Override public Bundle createBundle() {
        return wrap.createBundle();
    }
}
