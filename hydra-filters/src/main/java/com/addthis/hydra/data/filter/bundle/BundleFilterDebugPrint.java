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
package com.addthis.hydra.data.filter.bundle;

import java.util.concurrent.atomic.AtomicLong;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundlePrinter;
import com.addthis.codec.Codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">outputs debugging information</span>.
 * <p/>
 * <p>This filter will print out the contents of the first n bundles to the console output,
 * where n is determined by the parameter {@link #maxBundles maxBundles}.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op : "chain",
 *     filter : [
 *       {op : "debug"},
 *       // continue processing
 *     ]
 *   }</pre>
 *
 * @user-reference
 * @hydra-name debug
 */
public class BundleFilterDebugPrint extends BundleFilter {

    private static final Logger log = LoggerFactory.getLogger(BundleFilterDebugPrint.class);

    /**
     * A string prefix to pre-append to the debugging information. Default is "".
     */
    @Codec.Set(codable = true)
    private String prefix = "";

    /**
     * If true then this filter returns false. Default is false (ie. the filter returns true).
     */
    @Codec.Set(codable = true)
    private boolean fail = false;

    /**
     * Maximum number of bundles to print. Default is 100.
     */
    @Codec.Set(codable = true)
    private long maxBundles = 100;

    /**
     * Optionally specify to print a bundle every N seconds if
     * parameter is a positive integer. Default is 0.
     */
    @Codec.Set(codable = true)
    private long every = 0;

    private final AtomicLong bundleCounter = new AtomicLong();

    private final AtomicLong bundleTimer = new AtomicLong();

    // Should be used solely for unit testing.
    private String cacheOutput = null;

    // Should be used solely for unit testing.
    private boolean enableCacheOutput = false;

    @Override
    public void initialize() {
    }

    BundleFilterDebugPrint enableCacheOutput() {
        this.enableCacheOutput = true;
        return this;
    }

    String getCacheOutput() {
        return cacheOutput;
    }

    public static String formatBundle(Bundle bundle) {
        return BundlePrinter.printBundle(bundle);
    }

    private boolean testBundleTimer() {
        if (every <= 0) {
            return false;
        }
        while (true) {
            long current = JitterClock.globalTime();
            long previous = bundleTimer.get();
            if (current - previous > (every * 1000)) {
                if (bundleTimer.compareAndSet(previous, current)) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        boolean print = (bundleCounter.getAndIncrement() < maxBundles) || testBundleTimer();
        if (print) {
            String bundleString = formatBundle(bundle);
            if (Strings.isEmpty(prefix)) {
                log.info(bundleString);
            } else {
                log.info(prefix + " : " + bundleString);
            }
            if (enableCacheOutput) {
                cacheOutput = bundleString;
            }
        }

        return !fail;
    }
}
