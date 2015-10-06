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
import com.addthis.basis.util.LessStrings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundlePrinter;

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
 *    {debug.every: 10}
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterDebugPrint implements BundleFilter {

    private static final int MAX_BUNDLE_LIMIT = 1000;

    private static final Logger log = LoggerFactory.getLogger(BundleFilterDebugPrint.class);

    /**
     * A string prefix to pre-append to the debugging information. Default is "".
     */
    private final String prefix;

    /**
     * If true then this filter returns false. Default is false (ie. the filter returns true).
     */
    private final boolean fail;

    /**
     * Maximum number of bundles to print. Default is 100.
     */
    private final long maxBundles;

    /**
     * Optionally specify to print a bundle every N seconds if
     * parameter is a positive integer. Default is 0.
     */
    private final long every;

    @JsonCreator
    public BundleFilterDebugPrint(@JsonProperty("prefix") String prefix,
                                  @JsonProperty("fail") boolean fail,
                                  @JsonProperty("maxBundles") long maxBundles,
                                  @JsonProperty("every") long every) {
        Preconditions.checkArgument(maxBundles < MAX_BUNDLE_LIMIT, "Max bundles must be < " + MAX_BUNDLE_LIMIT);
        this.prefix = prefix;
        this.fail = fail;
        this.maxBundles = maxBundles;
        this.every = every;
    }

    private final AtomicLong bundleCounter = new AtomicLong();

    private final AtomicLong bundleTimer = new AtomicLong();

    // Should be used solely for unit testing.
    private String cacheOutput = null;

    // Should be used solely for unit testing.
    private boolean enableCacheOutput = false;

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
    public boolean filter(Bundle bundle) {
        boolean print = (bundleCounter.getAndIncrement() < maxBundles) || testBundleTimer();
        if (print) {
            String bundleString = formatBundle(bundle);
            if (LessStrings.isEmpty(prefix)) {
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
