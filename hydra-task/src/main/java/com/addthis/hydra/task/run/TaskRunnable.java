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
package com.addthis.hydra.task.run;

import com.addthis.codec.Codec;
import com.addthis.codec.Codec.ClassMap;
import com.addthis.codec.Codec.ClassMapFactory;
import com.addthis.hydra.task.hoover.Hoover;
import com.addthis.hydra.task.map.StreamMapper;
import com.addthis.hydra.task.treestats.TreeStatisticsJob;


/**
 * This is the specification for a Hydra job.
 *
 * @user-reference
 * @hydra-category
 */
@Codec.Set(classMapFactory = TaskRunnable.CMAP.class)
public abstract class TaskRunnable implements Codec.Codable {

    private static ClassMap cmap = new ClassMap() {
        @Override
        public String getClassField() {
            return "type";
        }

        @Override
        public String getCategory() {
            return "Hydra job";
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

    public static final void registerTask(String type, Class<? extends TaskRunnable> clazz) {
        cmap.add(type, clazz);
    }

    static {
        registerTask("hoover", Hoover.class);
        registerTask("map", StreamMapper.class);
        registerTask("treestats", TreeStatisticsJob.class);
    }

    public abstract void init(TaskRunConfig config);

    public abstract void exec();

    public abstract void terminate();

    public abstract void waitExit();
}
