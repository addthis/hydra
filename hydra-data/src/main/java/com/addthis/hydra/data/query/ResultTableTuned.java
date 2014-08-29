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
import java.io.IOException;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import com.addthis.basis.util.MemoryCounter;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.value.ValueObject;

import com.google.common.collect.ForwardingList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of DataTable that switches the underlying DataTable object
 * based on performance and resource constraints. Never used directly outside of
 * this package. TODO when not tipped keep a hash of added objects to prevent
 * double-counting TODO which can occur when 'depivot'ing a data set, for
 * example (row/col keys).
 */
public class ResultTableTuned extends ForwardingList<Bundle> implements DataTable, DataTableFactory {

    private static final Random random = new Random(System.currentTimeMillis());
    private static final Logger log = LoggerFactory.getLogger(ResultTableTuned.class);

    private DataTable result;
    private DataTableFactory factory;
    private File tempDir;
    private boolean tipped;
    private boolean cantip;
    private long memTip;
    private long rowTip;
    private int cells;
    private long estMem;

    protected ResultTableTuned(File tempDir,
                               long rowtip,
                               long memtip,
                               DataTableFactory factory,
                               int sizeHint) throws IOException {
        this.tempDir = tempDir;
        this.memTip = memtip;
        this.rowTip = rowtip;
        this.factory = factory;
        this.cantip = tempDir != null && tempDir.exists() && tempDir.isDirectory() &&
                      ((rowtip > 0) || (memtip > 0));
        this.tipped = !cantip;
        log.debug("creating RAT temp={}, rowTip={}, memTip={}, cantip={}, tipped={}", tempDir,
                  rowtip, memTip, cantip, tipped);
        if (cantip && (sizeHint >= rowtip) && (rowtip > 0)) {
            File tmp = createTempFile();
            tipped = true;
            result = new ResultTableDisk(factory, tmp);
            if (log.isDebugEnabled()) {
                log.debug(hashCode() + " creating result disk backed to " + tmp + " tip=" + rowtip + " sizeHint=" + sizeHint);
            }
        } else {
            result = new ResultTable(factory, sizeHint * 2);
        }
    }

    @Override protected List<Bundle> delegate() {
        return result;
    }

    @Override
    public String toString() {
        return "(RAT:" + (cantip ? "cantip" : "notip") + ":rt=" + rowTip + ":mt=" + memTip + ":" + result + ")";
    }

    protected void cleanup() {
        if (result instanceof ResultTableDisk) {
            ((ResultTableDisk) result).delete();
        }
    }

    private void estimateRow(Bundle row) {
        for (BundleField f : row.getFormat()) {
            ValueObject qv = row.getValue(f);
            if (qv != null) {
                cells++;
            }
        }
        if (memTip > 0) {
            estMem += MemoryCounter.estimateSize(row);
        }
    }

    private File createTempFile() {
        return new File(tempDir, "result." + Long.toHexString(random.nextLong()) + ".tmp");
    }

    private void tipCheck() {
        if (!tipped && ((rowTip > 0 && result.size() > rowTip) || (memTip > 0 && estMem > memTip))) {
            try {
                File tmp = createTempFile();
                if (log.isDebugEnabled()) {
                    log.debug(hashCode() + " tipping to " + tmp + " @ rows=" + result.size() + " cells=" + cells + " mem=" + estMem);
                }
                ResultTableDisk dbl = new ResultTableDisk(factory, tmp);
                dbl.append(result);
                result = dbl;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            tipped = true;
        }
    }

    @Override
    public DataTable createTable(int sizeHint) {
        return factory.createTable(sizeHint);
    }

    @Override public boolean add(Bundle element) {
        result.add(element);
        if (!tipped) {
            estimateRow(element);
        }
        tipCheck();
        return true;
    }

    @Override public void add(int index, Bundle element) {
        result.add(index, element);
        if (!tipped) {
            estimateRow(element);
        }
        tipCheck();
    }

    @Override public boolean addAll(Collection<? extends Bundle> collection) {
        return standardAddAll(collection);
    }

    @Override public boolean addAll(int index, Collection<? extends Bundle> collection) {
        return standardAddAll(index, collection);
    }

    @Override
    public Bundle createBundle() {
        return result.createBundle();
    }

    @Override
    public BundleFormat getFormat() {
        return result.getFormat();
    }

    @Override
    public void sort(Comparator<? super Bundle> comp) {
        result.sort(comp);
    }

    // DEPRECATED METHODS

    @Override public void append(Bundle row) {
        this.add(row);
    }

    @Override public void insert(int index, Bundle row) {
        this.add(index, row);
    }

    @Override public void append(DataTable result) {
        this.addAll(result);
    }
}
