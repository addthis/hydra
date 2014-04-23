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
package com.addthis.hydra.data.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.addthis.codec.Codec;

import org.apache.commons.lang3.ArrayUtils;

/**
 *         Tools for finding change points in an integer array.
 */
public class FindChangePoints implements Codec.Codable {

    /**
     * Finds high points in an array of integers.
     *
     * @param data       The array of integers in which to search
     * @param max_width  The maximum width to group high-points together
     * @param min_height The minimum height to consider
     * @return A list of pairs of integers of the form (index, size)
     */
    public static List<ChangePoint> findHighPoints(Long[] data, int max_width, int min_height) {
        List<ChangePoint> rv = new ArrayList<ChangePoint>();
        int currIndex = 0;
        long currHt = data[0];
        int currWidth = 0;
        boolean started = false;
        if (data.length <= 1) return rv;
        for (int i = 0; i < data.length; i++) {
            if (data[i] > currHt && data[i] > min_height) {
                started = true;
                currIndex = i;
                currHt = data[i];
                currWidth = 0;
            } else {
                if (started) {
                    currWidth++;
                    if (currWidth >= max_width) {
                        started = false;
                        rv.add(new ChangePoint(currHt, currIndex, ChangePoint.ChangePointType.PEAK));
                        currWidth = 0;
                        currIndex = -1;
                        currHt = 0;
                    }
                }
            }
        }
        if (started) {
            rv.add(new ChangePoint(currHt, currIndex, ChangePoint.ChangePointType.PEAK));
        }
        return rv;
    }

    /**
     * Finds places where the data changed dramatically, either sustained or "instantaneously"
     *
     * @param data The array of integers in which to search
     * @return A list of pairs of integers of the form (index, size)
     */
    public static List<ChangePoint> findSignificantPoints(Long[] data, int minChange, double minRatio, double minZScore, int inactiveThreshold, int windowSize) {
        List<ChangePoint> rv = new ArrayList<ChangePoint>();
        rv.addAll(findAndSmoothOverPeaks(data, minChange, minZScore, windowSize));
        rv.addAll(findChangePoints(data, minChange, minRatio, minZScore, inactiveThreshold, windowSize));
        return rv;
    }

    private static List<ChangePoint> findChangePoints(Long[] data, int minChange, double minRatio, double minZScore, int inactiveThreshold, int windowSize) {
        List<Long> dataList = Arrays.asList(data);
        ArrayList<ChangePoint> rvList = new ArrayList<ChangePoint>();
        for (int i = 2; i < data.length; i++) {
            int startIndex = Math.max(i - windowSize + 1, 0);
            Long[] currSlice = dataList.subList(startIndex, i).toArray(new Long[]{});
            long nextValue = data[i];
            double predicted = linearPredictNext(currSlice);
            double diff = nextValue - predicted;
            double zScoreDiff = diff / sd(currSlice);
            double changeRatio = -1 + (double) (nextValue) / Math.max(predicted, 1.);
            if (Math.abs(zScoreDiff) > minZScore && Math.abs(diff) > minChange && Math.abs(changeRatio) > minRatio) {
                ChangePoint.ChangePointType type = chooseTypeForChange((long) mean(currSlice), nextValue, inactiveThreshold);
                rvList.add(new ChangePoint((int) diff, i, type));
            }
        }
        return rvList;
    }

    private static ChangePoint.ChangePointType chooseTypeForChange(long before, long after, int inactiveThreshold) {
        if (before > after) {
            return after > inactiveThreshold ? ChangePoint.ChangePointType.FALL : ChangePoint.ChangePointType.STOP;
        } else {
            return before < inactiveThreshold ? ChangePoint.ChangePointType.START : ChangePoint.ChangePointType.RISE;
        }
    }

    private static List<ChangePoint> findAndSmoothOverPeaks(Long[] data, int minChange, double minZscore, int width) {
        ArrayList<ChangePoint> rvList = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            int leftEndpoint = Math.max(0, i - width);
            int rightEndpoint = Math.min(i + width, data.length);
            Long[] neighborhood = Arrays.copyOfRange(data, leftEndpoint, rightEndpoint);
            Long[] neighborhoodWithout = ArrayUtils.addAll(Arrays.copyOfRange(data, leftEndpoint, i), Arrays.copyOfRange(data, i + 1, rightEndpoint));
            if (sd(neighborhood) > minZscore * sd(neighborhoodWithout)) {
                double change = data[i] - mean(neighborhoodWithout);
                if (Math.abs(change) > minChange) {
                    rvList.add(new ChangePoint((int) change, i, ChangePoint.ChangePointType.PEAK));
                    data[i] = (long) mean(neighborhoodWithout);
                }
            }
        }
        return rvList;
    }

    private static int sum(Long[] longs) {
        int rv = 0;
        for (Long z : longs) {
            rv += z;
        }
        return rv;
    }

    public static double mean(Long[] longs) {
        return (double) (sum(longs)) / longs.length;
    }

    private static double sd(Long[] longs) {
        double mean = mean(longs);
        double sumSquareResiduals = 0;
        for (long z : longs) {
            sumSquareResiduals += Math.pow(mean - z, 2);
        }
        return Math.max(Math.sqrt(sumSquareResiduals), .0001);
    }

    private static double linearPredictNext(Long[] ints) {
        double slope;
        double intercept;
        int len = ints.length;
        Long[] xx = new Long[len];
        Long[] xy = new Long[len];
        for (int i = 0; i < len; i++) {
            xx[i] = (long) (i * i);
            xy[i] = i * ints[i];
        }
        double meanx = .5 * (len - 1.);
        slope = (mean(xy) - meanx * mean(ints)) / (mean(xx) - Math.pow(meanx, 2));
        intercept = mean(ints) - slope * meanx;
        return slope * ints.length + intercept;
    }
}
