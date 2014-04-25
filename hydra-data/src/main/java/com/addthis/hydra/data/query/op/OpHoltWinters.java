package com.addthis.hydra.data.query.op;

import com.addthis.basis.util.Strings;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.bundle.table.DataTable;
import com.addthis.bundle.table.DataTableFactory;
import com.addthis.bundle.util.BundleColumnBinder;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.query.AbstractTableOp;
import com.addthis.hydra.data.query.QueryStatusObserver;
import com.addthis.hydra.data.util.HoltWinters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>This query operation <span class="hydra-summary"> applies Holt-Winters algorithm</span>.
 * <p/>
 * <p>Holt-Winters is an expontnetial smoothing function that works well for forecasting
 * time series data sets</p>
 *
 * <p>See {@link com.addthis.hydra.data.util.HoltWinters} for more information on the algorithm</p>
 *
 * <p>The operation takes a colon separated list of optional parameters:</p>
 *
 * <ul>
 *     <li>timeColumn</li>
 *     <li>valColumn</li>
 *     <li>alpha - Exponential smoothing coefficients for level, trend, seasonal components.</li>
 *     <li>beta - Exponential smoothing coefficients for level, trend, seasonal components.</li>
 *     <li>gamma - Exponential smoothing coefficients for level, trend, seasonal components.</li>
 *     <li>period - A complete season's data consists of L periods. And we need
 *            to estimate the trend factor from one period to the next. To
 *            accomplish this, it is advisable to use two complete seasons;
 *            that is, 2L periods.</li>
 *     <li>m - extrapolated future data points</li>
 * </ul>
 *
 * <p>The operation returns a {@link com.addthis.bundle.table.DataTable} with 4 columns.</p>
 *
 * <ul>
 *     <li>timeColumn</li>
 *     <li>valColumn</li>
 *     <li>forecast</li>
 *     <li>error percentage</li>
 * </ul>
 *
 * <p/>
 * <pre>
 *
 * Example:
 *
 * holtwinters=0:1:.1:.1:.1:7:3
 *
 * </pre>
 *
 * @user-reference
 * @hydra-name holwinters
 */
public class OpHoltWinters extends AbstractTableOp {

    private Logger log = LoggerFactory.getLogger(OpHoltWinters.class);

    private int timeColumn = 0;
    private int valColumn = 1;
    private double alpha = .1;
    private double beta = .1;
    private double gamma = .1;
    private int period = 7;
    private int futureDataPoints = 3;

    public OpHoltWinters(DataTableFactory tableFactory, String args, QueryStatusObserver queryStatusObserver) {
        super(tableFactory, queryStatusObserver);
        try {
            if (args.length() > 0) {
                String[] opt = args.split(":");
                timeColumn = opt.length >= 1 ? Integer.parseInt(opt[0]) : 0;
                valColumn = opt.length >= 2 ? Integer.parseInt(opt[1]) : 1;
                alpha = opt.length >= 3 ? Double.parseDouble(opt[2]) : .1;
                beta = opt.length >= 4 ? Double.parseDouble(opt[3]) : .1;
                gamma = opt.length >= 5 ? Double.parseDouble(opt[4]) : .1;
                period = opt.length >= 6 ? Integer.parseInt(opt[5]) : 7;
                futureDataPoints = opt.length >= 7 ? Integer.parseInt(opt[6]) : 3;
            }
            log.info("Initiated HoltWinters with parameters " +
                    Strings.join(new Object[]{timeColumn, valColumn, alpha, beta, gamma, period, futureDataPoints}, ","));
        } catch (Exception ex) {
            log.error("", ex);
        }
    }

    @Override
    public DataTable tableOp(DataTable result) {
        if (result == null || result.size() == 0) {
            return result;
        }
        if (result.size() < 2*period) {
            throw new RuntimeException("Input rows must be > 2*period, rows=" + result.size() + " period=" + period);
        }
        long[] data = new long[result.size()];
        BundleField[] fields = new BundleColumnBinder(result.get(0)).getFields();
        BundleField timeField = fields[timeColumn];
        for (int i = 0; i < result.size(); i++) {
            data[i] = result.get(i).getValue(fields[valColumn]).asLong().getLong();
        }
        double[] forecast = HoltWinters.forecast(data, alpha, beta, gamma,
                period, futureDataPoints, false);
        DataTable table = createTable(forecast.length);
        ListBundleFormat format = new ListBundleFormat();
        BundleField outTimeField = format.getField("time");
        BundleField actualField = format.getField("actual");
        BundleField outSizeField = format.getField("forecast");
        BundleField error = format.getField("error");
        int index = 0;
        for (double f : forecast) {
            if (index + 1 > result.size()) {
                break;
            }
            Bundle row = new ListBundle(format);
            Bundle b = result.get(index++);
            ValueObject actual = b.getValue(fields[valColumn]);
            long actualLong = actual.asLong().getLong();
            row.setValue(outTimeField, b.getValue(timeField));
            row.setValue(actualField, actual);
            row.setValue(outSizeField, ValueFactory.create(f));
            row.setValue(error, ValueFactory.create(((f - actualLong)/Math.max(f, actualLong)) * 100));
            table.append(row);
        }
        return table;
    }
}
