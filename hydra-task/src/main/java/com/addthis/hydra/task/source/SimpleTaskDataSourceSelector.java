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

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SimpleTaskDataSourceSelector implements TaskDataSourceSelector {

    private static final Logger log = LoggerFactory.getLogger(SimpleTaskDataSourceSelector.class);

    @Override
    public TaskDataSource selectSource(Collection<TaskDataSource> sources, TaskDataSource last) {
        if (sources == null || sources.isEmpty()) {
            return null;
        }
        for (TaskDataSource taskDataSource : sources) {
            try {
                if (taskDataSource.peek() != null) {
                    return taskDataSource;
                } else {
                    // source is complete so close it
                    taskDataSource.close();
                }
            } catch (Exception e) {
                log.warn("Error peeking at dataChannelSource", e);
                }
        }
        return null;
    }
}
