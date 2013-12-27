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

import java.util.LinkedList;

@SuppressWarnings("serial")
public class SlidingWindow extends LinkedList<Long> {

    public SlidingWindow(int size) {
        max = size;
    }

    private final int max;
    private long sum;

    public synchronized long syncAppend(long value) {
        return append(value);
    }

    public long append(long value) {
        addLast(value);
        sum += value;
        if (size() >= max) {
            sum -= removeFirst();
        }
        return sum;
    }

    public synchronized long syncMin() {
        return min();
    }

    public long min() {
        long min = peek();
        for (long v : this) {
            min = Math.min(min, v);
        }
        return min;
    }

    public synchronized long syncMax() {
        return max();
    }

    public long max() {
        Long max = peek();
        if (max == null) {
            return 0;
        }
        for (long v : this) {
            max = Math.max(max, v);
        }
        return max;
    }

    public long sum() {
        return sum;
    }

    public long average() {
        return size() > 0 ? sum / size() : 0;
    }
}
