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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundlePrinter;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.json.CodecJSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">executes a chain of bundle filters</span>.
 * <p/>
 *
 * @user-reference
 * @hydra-name chain
 */
public class BundleFilterChain extends BundleFilter {

    private static final Logger log = LoggerFactory.getLogger(BundleFilterChain.class);

    /**
     * The chain of bundle filters to execute.
     */
    @FieldConfig(codable = true, required = true)
    private BundleFilter[] filter;

    /**
     * If true then stop execution on the failure of a filter. Default is true.
     */
    @FieldConfig(codable = true)
    private boolean failStop = true;

    /**
     * The value to return on failure if {@link #failStop failStop} is true. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean failReturn = false;

    /**
     * If true then print out debugging information. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean debug;

    /**
     * Maximum number of bundles to print when
     * {@link #debug} is true. Default is 100.
     */
    @FieldConfig(codable = true)
    private long debugMaxBundles = 100;

    private final AtomicLong bundleCounter = new AtomicLong();

    @Override
    public void open() {
        for (BundleFilter f : filter) {
            f.open();
        }
    }

    @Override
    public boolean filter(Bundle row) {
        for (BundleFilter f : filter) {
            if (!f.filter(row) && failStop) {
                if (debug && bundleCounter.getAndIncrement() < debugMaxBundles) {
                    log.warn("fail @ " + CodecJSON.tryEncodeString(f, "UNKNOWN") + " with " +
                             BundlePrinter.printBundle(row));
                }
                return failReturn;
            }
        }
        return true;
    }
}
