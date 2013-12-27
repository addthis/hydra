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

import com.addthis.bundle.channel.DataChannelOutput;
import com.addthis.codec.Codec;
import com.addthis.codec.Codec.ClassMap;
import com.addthis.codec.Codec.ClassMapFactory;
import com.addthis.hydra.common.plugins.PluginReader;
import com.addthis.hydra.task.run.TaskRunConfig;


/**
 * This section of the job specification handles output sinks.
 * <p/>
 * <p>Data sinks are responsible for emitting output.</p>
 *
 * @user-reference
 * @hydra-category
 */
@Codec.Set(classMapFactory = TaskDataOutput.CMAP.class)
public abstract class TaskDataOutput implements DataChannelOutput {

    private static ClassMap cmap = new ClassMap() {
        @Override
        public String getClassField() {
            return "type";
        }

        @Override
        public String getCategory() {
            return "output sink";
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

    @SuppressWarnings("unused")
    public static final void registerDataOutput(String type, Class<? extends TaskDataOutput> clazz) {
        cmap.add(type, clazz);
    }

    static {
        PluginReader.registerPlugin("-output-sinks.classmap", cmap, TaskDataOutput.class);
    }

    protected abstract void open(TaskRunConfig config);

    public final void init(TaskRunConfig config) {
        open(config);
    }
}
