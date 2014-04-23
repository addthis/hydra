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
package com.addthis.hydra.data.query.op;

import java.util.List;

import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.query.QueryStatusObserver;
import com.addthis.hydra.data.util.ChangePoint;
import com.addthis.hydra.data.util.FindChangePoints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detect significant changes within a column of a DataTable.
 */
public class OpChangePoints extends AbstractTableOp {

    private Logger log = LoggerFactory.getLogger(OpChangePoints.class);
    int timeColumn;
    int valColumn; // Only one column is supported at the moment
    int minChange;
    double minRatio;
    double minZScore;
    int inactiveThreshold;
    int windowSize;

    public OpChangePoints(DataTableFactory factory, String args, QueryStatusObserver queryStatusObserver) {
        super(factory, queryStatusObserver);
        try {
            String[] opt = args.split(":");
            timeColumn = opt.length >= 1 ? Integer.parseInt(opt[0]) : 0;
            valColumn = opt.length >= 2 ? Integer.parseInt(opt[1]) : 1;
            minChange = opt.length >= 3 ? Integer.parseInt(opt[2]) : 10;
            minRatio = opt.length >= 4 ? Double.parseDouble(opt[3]) : .3;
            minZScore = opt.length >= 5 ? Double.parseDouble(opt[4]) : 1.5;
            inactiveThreshold = opt.length >= 6 ? Integer.parseInt(opt[5]) : 1;
            windowSize = opt.length >= 7 ? Integer.parseInt(opt[6]) : 5;
            log.info("Initiated changepoints with parameters " +
                     Strings.join(new Object[]{valColumn, minChange, minRatio, minZScore, inactiveThreshold}, ","));
        } catch (Exception ex) {
            log.error("", ex);
        }
    }

    /**
     * Runs FindChangePoints on a column of the data table.
     *
     * @param result Input data table
     * @return A list of significant change points, giving the time, type, and size of each change
     */
    @Override
    public DataTable tableOp(final DataTable result) {
        if (result == null || result.size() == 0) {
            return result;
        }
        Long[] data = new Long[result.size()];
        BundleField[] fields = new BundleColumnBinder(result.get(0)).getFields();
        BundleField timeField = fields[timeColumn];
        for (int i = 0; i < result.size(); i++) {
            data[i] = result.get(i).getValue(fields[valColumn]).asLong().getLong();
        }
        List<ChangePoint> changePoints = FindChangePoints.findSignificantPoints(data, minChange, minRatio, minZScore, inactiveThreshold, windowSize);
        DataTable table = createTable(changePoints.size());
        ListBundleFormat format = new ListBundleFormat();
        BundleField outTimeField = format.getField("time");
        BundleField outTypeField = format.getField("type");
        BundleField outSizeField = format.getField("size");
        for (ChangePoint pt : changePoints) {
            Bundle row = new ListBundle(format);
            row.setValue(outTimeField, result.get(pt.getIndex()).getValue(timeField));
            row.setValue(outTypeField, ValueFactory.create(pt.getType().toString()));
            row.setValue(outSizeField, ValueFactory.create(pt.getSize()));
            table.append(row);
        }
        return table;
    }
}
