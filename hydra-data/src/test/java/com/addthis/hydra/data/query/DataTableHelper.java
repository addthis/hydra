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

import java.io.File;

import java.util.Comparator;
import java.util.Iterator;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;


@SuppressWarnings("serial")
public class DataTableHelper implements DataTable {

    private DataTable result;
    private Bundle currentRow;
    private int currentColumn;

    public DataTableHelper() {
        this(0);
    }

    public DataTableHelper(int sizeHint) {
        this(sizeHint, null, 0);
    }

    public DataTableHelper(File tempDir, int tipToDisk) {
        this(0, null, 0);
    }

    public DataTableHelper(int sizeHint, File tempDir, int rowTip) {
        // TODO BROKEN
        result = new QueryOpProcessor.Builder(null, null)
                .rowTip(rowTip).tempDir(tempDir).build().createTable(sizeHint);
    }

    public DataTableHelper td(long val) {
        td(ValueFactory.create(val));
        return this;
    }

    public DataTableHelper td(double val) {
        td(ValueFactory.create(val));
        return this;
    }

    public DataTableHelper td(String v1) {
        td(ValueFactory.create(v1));
        return this;
    }

    public DataTableHelper td(String v1, String v2) {
        td(ValueFactory.create(v1));
        td(ValueFactory.create(v2));
        return this;
    }

    public DataTableHelper td(String v1, String v2, String v3) {
        td(ValueFactory.create(v1));
        td(ValueFactory.create(v2));
        td(ValueFactory.create(v3));
        return this;
    }

    public DataTableHelper td(String v1, String v2, String v3, String v4) {
        td(ValueFactory.create(v1));
        td(ValueFactory.create(v2));
        td(ValueFactory.create(v3));
        td(ValueFactory.create(v4));
        return this;
    }

    public DataTableHelper td(String v1, String v2, String v3, String v4, String v5) {
        td(ValueFactory.create(v1));
        td(ValueFactory.create(v2));
        td(ValueFactory.create(v3));
        td(ValueFactory.create(v4));
        td(ValueFactory.create(v5));
        return this;
    }

    public DataTableHelper td(String v1, String v2, String v3, String v4, String v5, String v6) {
        td(ValueFactory.create(v1));
        td(ValueFactory.create(v2));
        td(ValueFactory.create(v3));
        td(ValueFactory.create(v4));
        td(ValueFactory.create(v5));
        td(ValueFactory.create(v6));
        return this;
    }

    public DataTableHelper td() {
        return tdNull();
    }

    public DataTableHelper tdNull() {
        td((ValueObject) null);
        return this;
    }

    public DataTableHelper td(ValueObject val) {
        if (size() == 0) {
            tr();
        }
        BundleField field = getFormat().getField((currentColumn++) + "");
        if (val != null) {
            currentRow.setValue(field, val);
        }
        return this;
    }

    public DataTableHelper tr() {
        currentRow = createBundle();
        append(currentRow);
        currentColumn = 0;
        return this;
    }

    @Override
    public void append(Bundle row) {
        currentRow = row;
        currentColumn = 0;
        result.append(row);
    }

    @Override
    public void append(DataTable result) {
        result.append(result);
    }

    @Override
    public Bundle get(int rownum) {
        return result.get(rownum);
    }

    @Override
    public void insert(int index, Bundle row) {
        result.insert(index, row);
    }

    @Override
    public Bundle remove(int index) {
        return result.remove(index);
    }

    @Override
    public Bundle set(int rownum, Bundle row) {
        return result.set(rownum, row);
    }

    @Override
    public int size() {
        return result.size();
    }

    @Override
    public void sort(Comparator<Bundle> comp) {
        result.sort(comp);
    }

    @Override
    public Iterator<Bundle> iterator() {
        return result.iterator();
    }

    @Override
    public Bundle createBundle() {
        return result.createBundle();
    }

    @Override
    public BundleFormat getFormat() {
        return result.getFormat();
    }
}
