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

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleFormat;
import com.addthis.bundle.core.list.ListBundleFormat;
import com.addthis.codec.annotations.Pluggable;


/**
 * This section of the job specification handles output sinks.
 * <p/>
 * <p>Data sinks are responsible for emitting output.</p>
 *
 * @user-reference
 * @hydra-category Output Sinks
 * @hydra-doc-position 5
 */
@Pluggable("output-sink")
public abstract class TaskDataOutput implements DataChannelOutput {

    protected final BundleFormat format;

    protected TaskDataOutput() {
        format = new ListBundleFormat();
    }

    protected TaskDataOutput(BundleFormat format) {
        this.format = format;
    }

    protected abstract void open();

    @Override public void send(List<Bundle> bundles) {
        for (Bundle bundle : bundles) {
            send(bundle);
        }
    }

    @Override public Bundle createBundle() {
        return format.createBundle();
    }

    public final void init() {
        open();
    }
}
