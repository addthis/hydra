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
package com.addthis.hydra.task.output;

import java.util.List;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * This output sink <span class="hydra-summary">applies a filter before writing to output</span>.
 *
 * @user-reference
 * @hydra-name filtered
 */
public class FilteredDataOutput extends TaskDataOutput {

    private static final Logger log = LoggerFactory.getLogger(FilteredDataOutput.class);

    /**
     * Underlying output sink to which data is written.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private TaskDataOutput output;

    /**
     * Bundle filter that will be applied to each bundle
     * prior to writing to output.
     * This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private BundleFilter filter;

    @Override
    protected void open() {
        output.open();
    }

    @Override
    public void send(Bundle bundle) throws DataChannelError {
        if (filter.filter(bundle)) {
            output.send(bundle);
        }
    }

    @Override
    public void sendComplete() {
        output.sendComplete();
    }

    @Override
    public void sourceError(Throwable er) {
        output.sourceError(er);
    }

    @Override
    public Bundle createBundle() {
        return output.createBundle();
    }

    @Override
    public void send(List<Bundle> bundles) {
        for (Bundle bundle : bundles) {
            send(bundle);
        }
    }
}

