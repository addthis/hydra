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
package com.addthis.hydra.task.source;

import com.addthis.bundle.channel.DataChannelSource;
import com.addthis.bundle.core.BundleField;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.annotations.Pluggable;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.task.output.OutputRootDirectories;

/**
 * This section of the job specification handles input sources.
 * <p/>
 * <p>Data sources are responsible for providing a stream of Bundle objects.</p>
 *
 * @user-reference
 * @hydra-category Input Sources
 * @hydra-doc-position 2
 * @exclude-fields shardField, enabled
 */
@Pluggable("input-source")
public abstract class TaskDataSource implements Codable, DataChannelSource, OutputRootDirectories, Cloneable {

    /**
     * Optionally specify a field that will be used as input to a
     * hash function to determine which input processing thread to use. Default is null.
     */
    @FieldConfig(codable = true)
    private BundleField shardField;

    /** If false then disable this data source. Default is true. */
    @FieldConfig(codable = true)
    private boolean enabled = true;

    public abstract void init();

    public final BundleField getShardField() {
        return shardField;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
