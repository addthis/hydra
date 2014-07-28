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
package com.addthis.hydra.data.filter.closeablebundle;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * execute a bundle filter chain.
 * optionally exit on a filter failure
 */
public class CloseableBundleFilterChain extends CloseableBundleFilter {

    private static final Logger log = LoggerFactory.getLogger(CloseableBundleFilterChain.class);

    @FieldConfig(codable = true, required = true)
    private CloseableBundleFilter[] filter;
    @FieldConfig(codable = true)
    private boolean failStop   = true;
    @FieldConfig(codable = true)
    private boolean failReturn = false;
    @FieldConfig(codable = true)
    private boolean debug;

    @Override
    public void initialize() {
        for (CloseableBundleFilter f : filter) {
            f.initOnceOnly();
        }
    }

    @Override
    public boolean filterExec(Bundle row) {
        for (BundleFilter f : filter) {
            if (!f.filterExec(row) && failStop) {
                if (debug) {
                    log.warn("fail @ " + CodecJSON.encodeString(f));
                }
                return failReturn;
            }
        }
        return true;
    }

    @Override
    public void close() {
        for (CloseableBundleFilter f : filter) {
            f.close();
        }
    }
}
