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
import com.addthis.codec.Codec;
import com.addthis.codec.Codec.ClassMap;
import com.addthis.codec.Codec.ClassMapFactory;
import com.addthis.hydra.common.plugins.PluginReader;
import com.addthis.hydra.task.run.TaskRunConfig;

/**
 * This section of the job specification handles input sources.
 * <p/>
 * <p>Data sources are responsible for providing a stream of Bundle objects.</p>
 *
 * @user-reference
 * @hydra-category
 * @exclude-fields shardField, enabled
 */
@Codec.Set(classMapFactory = TaskDataSource.CMAP.class)
public abstract class TaskDataSource implements Codec.Codable, DataChannelSource, Cloneable {

    private static ClassMap cmap = new ClassMap() {
        @Override
        public String getClassField() {
            return "type";
        }

        @Override
        public String getCategory() {
            return "input source";
        }

    };

    /**
     * @exclude
     */
    public static class CMAP implements ClassMapFactory {

        public ClassMap getClassMap() {
            return cmap;
        }
    }

    static {
        PluginReader.registerPlugin("-input-sources.classmap", cmap, TaskDataSource.class);
    }

    /**
     * Optionally specify a field that will be used as input to a
     * hash function to determine which input processing thread to use. Default is null.
     */
    @Codec.Set(codable = true)
    private BundleField shardField;

    /**
     * If false then disable this data source. Default is true.
     */
    @Codec.Set(codable = true)
    private boolean enabled = true;

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * sources are not required to implement this.  it is a hint to the job
     * manager that this source could be used again (task re-kicked) and that
     * more data would be available.  StreamSourceMeshy returns true when a
     * max data range for a single run has been reached.  It is only valid to
     * call this once next() has returned null.
     *
     * @return true if source exited prematurely (returned null on next()) but had more data.
     */
    public boolean hadMoreData() {
        return false;
    }

    protected abstract void open(TaskRunConfig config);

    public final void init(TaskRunConfig config) {
        open(config);
    }

    public final BundleField getShardField() {
        return shardField;
    }

    public TaskDataSource clone() {
        try {
            return (TaskDataSource) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
