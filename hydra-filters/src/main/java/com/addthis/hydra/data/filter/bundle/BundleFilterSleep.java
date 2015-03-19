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

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link com.addthis.hydra.data.filter.bundle.BundleFilter BundleFilter} <span class="hydra-summary">sleeps for N milliseconds</span>.
 * <p/>
 * The job will sleep for N milliseconds per bundle that is processed.
 *
 * @user-reference
 */
public class BundleFilterSleep implements BundleFilter {

    private final Logger log = LoggerFactory.getLogger(BundleFilterSleep.class);

    /**
     * Number of milliseconds to sleep. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private int duration;

    @Override
    public boolean filter(Bundle row) {
        try {
            if (duration > 0) {
                Thread.sleep(duration);
            }
        } catch (InterruptedException ex) {
            log.info(ex.toString());
        }
        return true;
    }
}
