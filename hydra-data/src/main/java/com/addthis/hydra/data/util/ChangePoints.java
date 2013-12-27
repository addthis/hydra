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
import java.util.Random;
import java.util.TreeMap;

import com.addthis.codec.Codec;

/**
 *         Tools for finding change points in an integer array.
 */
public class ChangePoints implements Codec.Codable {

    /**
     * Finds statistically significant changes in an Integer Array
     * Returns a Map: keys are indices of big changes
     * vals are estimated sizes of these changes
     */
    public static TreeMap<Integer, Integer> findChangePoints(Integer[] data, double threshold_ratio, int min_size, double inactive_threshold) {
        ArrayList<Integer> candidates = FCPhelper(data, 0, data.length - 1);
        return checkCandidates(candidates, data, threshold_ratio, min_size);
    }

    public static TreeMap<Integer, Integer> findPeaks2(Integer[] data, int max_width, int min_height) {
        TreeMap<Integer, Integer> rv = new TreeMap<Integer, Integer>();
        int currIndex = 0;
        int currHt = data[0];
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
                        rv.put(currIndex, currHt);
                        currWidth = 0;
                        currIndex = -1;
                        currHt = 0;
                    }
                }
            }
        }
        if (started) {
            rv.put(currIndex, currHt);
        }
        return rv;
    }

    public static TreeMap<Integer, Integer> findPeaks(Integer[] data, int min_size) {
        TreeMap<Integer, Integer> rv = new TreeMap<Integer, Integer>();
        Integer lastPeak = -1;
        for (int i = 1; i < data.length; i++) {
            int lb, rb;
            if (i < 3 || i > data.length - 5) {
                continue;
            }

            lb = i - 3;
            rb = i + 3;
            Integer[] leftNbhd = slice(data, lb, i - 1);
            Integer[] rightNbhd = slice(data, i + 1, rb);
            Integer del = differedPeaky(leftNbhd, rightNbhd, data[i]);
            if (Math.abs(del) > min_size) // A peak of width 1
            {
                rv.put(i, del);
            } else // A peak of width 2
            {
                Integer pairMean = (data[i] + data[i + 1]) / 2;
                leftNbhd = slice(data, lb, i - 1);
                rightNbhd = slice(data, i + 2, rb + 1);
                del = differedPeaky(leftNbhd, rightNbhd, pairMean);
                if (Math.abs(del) > min_size) // A peak of width 1
                {
                    rv.put(i, del);
                }
            }
        }
        return rv;
    }

    private static int differedPeaky(Integer[] A, Integer[] B, Integer z) {
        // Switch to a linear regression approach?
        double diffA = z - mean(A);
        double diffB = z - mean(B);
        if (Math.abs(diffA) > 3 * Math.sqrt(var(A))
            && Math.abs(diffB) > 3 * Math.sqrt(var(B))
            && (diffA > 0) == (diffB > 0))

        {
            return (int) diffA;
        } else {
            return 0;
        }

    }

    public static TreeMap<String, Integer> findChangePointsDetails(Integer[] data, double threshold_ratio, int min_size, int inactive_threshold) {
        ArrayList<Integer> candidates = FCPhelper(data, 0, data.length - 1);
        return checkAndSortCandidates(candidates, data, threshold_ratio, min_size, inactive_threshold);
    }

    private static ArrayList<Integer> FCPhelper(Integer[] data, int a, int b) {
        ArrayList<Integer> rv = new ArrayList<Integer>();
        if (b > a) {
            Integer[] currSlice = slice(data, a, b);
            if (changeOccured(currSlice)) {
                int index = findMinMSE(currSlice) + a;
                if (index - 1 > a) {
                    ArrayList<Integer> firstCPs = FCPhelper(data, a, index - 1);
                    for (int z : firstCPs) {
                        rv.add(z);
                    }
                }
                rv.add(index);
                if (index + 1 < b) {
                    ArrayList<Integer> secondCPs = FCPhelper(data, index + 1, b);
                    for (int z : secondCPs) {
                        rv.add(z);
                    }
                }
            }
        }
        return rv;
    }

    private static TreeMap<Integer, Integer> checkCandidates(ArrayList<Integer> cands, Integer[] data, double threshold_ratio, int min_size) {
        TreeMap<Integer, Integer> rv = new TreeMap<Integer, Integer>();
        for (int i = 0; i < cands.size(); i++) {
            int z = cands.get(i);
            int lb, rb;
            if (z < 2 || z > data.length - 4) {
                continue;
            }

            if (i == 0) {
                lb = Math.min(z - 2, z - z / 4);
            } else {
                int prevCand = cands.get(i - 1);
                lb = Math.min(z - 2, z - (z - prevCand) / 4);
            }
            if (i == cands.size() - 1) {
                rb = Math.max(z + 3, z + (data.length - z) / 4);
            } else {
                int nextCand = cands.get(i + 1);
                rb = Math.max(z + 3, z + (nextCand - z) / 4);
            }
            int del = differed(slice(data, lb, z), slice(data, z + 1, rb), threshold_ratio);
            if (Math.abs(del) > min_size) {
                rv.put(z, del);
            }
        }
        return rv;
    }

    private static TreeMap<String, Integer> checkAndSortCandidates(ArrayList<Integer> cands, Integer[] data, double threshold_ratio, int min_size, int inactive_threshold) {
        TreeMap<String, Integer> rv = new TreeMap<String, Integer>();
        for (int i = 0; i < cands.size(); i++) {
            Integer z = cands.get(i);
            int lb, rb;
            if (z < 2 || z > data.length - 4) {
                continue;
            }

            if (i == 0) {
                lb = Math.min(z - 2, z - z / 4);
            } else {
                int prevCand = cands.get(i - 1);
                lb = Math.min(z - 2, z - (z - prevCand) / 4);
            }
            if (i == cands.size() - 1) {
                rb = Math.max(z + 3, z + (data.length - z) / 4);
            } else {
                int nextCand = cands.get(i + 1);
                rb = Math.max(z + 3, z + (nextCand - z) / 4);
            }
            int del = differed(slice(data, lb, z), slice(data, z + 1, rb), threshold_ratio);
            // Currently recalculates means.
            double meanA = mean(slice(data, lb, z));
            double meanB = mean(slice(data, z + 1, rb));
            if (Math.abs(del) > min_size) {
                rv.put(findChangeType(meanA, meanB, inactive_threshold) + z, Math.abs(del));
            }
        }
        return rv;
    }

    /**
     * Uses Linear Regression to test whether the second array of data is out
     * of line with the trend in the first array. Returns an estimate for the size
     * of the change if a change is detected, and zero otherwise.
     */
    private static int differed(Integer[] A, Integer[] B, Double threshold_ratio) {
        double sdA = Math.sqrt(var(A));
        double[] lr = linReg(A);
        double meanA = mean(A);
        double meanB = mean(B);
        double biggerMean = Math.max(Math.abs(meanA), Math.abs(meanB));
        double diff = meanB - (lr[0] * (A.length + (double) (B.length - 1) / 2) + lr[1]);
        if (Math.abs(diff) > 3 * sdA && Math.abs(diff) > threshold_ratio * biggerMean) {
            return (int) (meanB - meanA);
        } else {
            return 0;
        }

    }

    private static String findChangeType(double meanA, double meanB, int inactive_threshold) {
        if (meanA < meanB) {
            if (meanA < inactive_threshold) {
                return "B"; // Begin
            } else {
                return "R"; // Rise
            }
        } else {
            if (meanB < inactive_threshold) {
                return "S"; // Stop
            } else {
                return "F"; // Fall
            }
        }
    }

    private static Integer[] slice(Integer[] data, int a, int b) {
        int l = b - a + 1;
        Integer[] rv = new Integer[l];
        for (int i = 0; i < l; i++) {
            rv[i] = data[a + i];
        }
        return rv;
    }

    private static boolean changeOccured(Integer[] data) {
        double CONFIDENCE_LEVEL = .9;
        int BOOTSTRAPS = 200;
        double mean = mean(data);
        double[] cs = cusum(data, mean);
        double Sdiff = range(cs);
        int numLess = 0;
        for (int i = 0; i < BOOTSTRAPS; i++) {
            double S0diff = range(cusum(bootstrap(data), mean));
            numLess += (S0diff < Sdiff ? 1 : 0);
        }
        return (double) (numLess) / BOOTSTRAPS > CONFIDENCE_LEVEL;
    }

    private static Integer[] bootstrap(Integer[] data) {
        Random random = new Random();
        Integer[] copy = data.clone();
        int l = data.length;
        for (int i = 0; i < data.length; i++) {
            int r = random.nextInt(l);
            Integer temp = copy[i];
            copy[i] = copy[r];
            copy[r] = temp;
        }
        return copy;
    }

    private static double[] cusum(Integer[] data, double m) {
        int l = data.length;
        double[] cusum = new double[l];
        cusum[0] = data[0] - m;
        for (int i = 1; i < l; i++) {
            cusum[i] = cusum[i - 1] + data[i] - m;
        }
        return cusum;
    }

    private static double range(double[] A) {
        double[] minmax = minmax(A);
        return minmax[1] - minmax[0];
    }

    private static double[] minmax(double[] A) {
        double min = A[0];
        double max = A[0];
        for (int i = 1; i < A.length; i++) {
            min = A[i] < min ? A[i] : min;
            max = A[i] > max ? A[i] : max;
        }
        return new double[]
                {
                        min, max
                };
    }

    public static double mean(Integer[] A) {
        return (double) (sum(A)) / A.length;
    }

    private static double var(Integer[] A) {
        double m = mean(A);
        double sum = 0;
        for (Integer z : A) {
            sum += Math.pow(z - m, 2);
        }
        return sum;
    }

    private static Long sum(Integer[] data) {
        long sum = 0;
        for (int i = 0; i < data.length; i++) {
            sum += data[i];
        }
        return sum;
    }

    private static int findMinMSE(Integer[] data) {
        int minInd = 0;
        double minVal = MSE(data, 0);
        for (int i = 1; i < data.length; i++) {
            double newMSE = MSE(data, i);
            if (newMSE < minVal) {
                minInd = i;
                minVal = newMSE;
            }
        }
        return minInd;
    }

    private static double MSE(Integer[] data, int i) {
        Integer[] firstPart = slice(data, 0, i);
        Integer[] secondPart = slice(data, i + 1, data.length - 1);
        double firstSum = 0;
        double secondSum = 0;
        double firstMean = mean(firstPart);
        double secondMean = mean(secondPart);
        for (Integer z : firstPart) {
            firstSum += Math.pow(z - firstMean, 2);
        }
        for (Integer z : secondPart) {
            secondSum += Math.pow(z - secondMean, 2);
        }
        return firstSum + secondSum;
    }

    /**
     * Finds a,b such that ax+b is a good model for Data
     */
    private static double[] linReg(Integer[] Data) {
        double[] rv = new double[2];
        int l = Data.length;
        Integer[] xy = new Integer[l];
        for (int i = 0; i < l; i++) {
            xy[i] = i * Data[i];
        }
        Integer[] xx = new Integer[l];
        for (int i = 0; i < l; i++) {
            xx[i] = i * i;
        }
        Integer sumx = l * (l - 1) / 2;
        rv[0] = (sum(xy) - (double) (sumx * sum(Data)) / l) / (sum(xx) - (double) (sumx * sumx) / l);
        rv[1] = mean(Data) - rv[0] * (double) (l) / 2;
        return rv;
    }
}
