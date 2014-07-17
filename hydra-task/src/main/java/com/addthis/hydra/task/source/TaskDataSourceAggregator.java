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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.task.run.TaskRunConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Wraps n DataChannelSource objects.  Clients can use this class to pull data
 * from several sources without having to worry about interacting with each source.
 */
public class TaskDataSourceAggregator extends TaskDataSource {

    private static final Logger log = LoggerFactory.getLogger(TaskDataSourceAggregator.class);

    private final TaskDataSourceSelector sourceSelector;
    private Queue<TaskDataSource> sourceList; // input sources
    private HashMap<TaskDataSource, TaskDataSource> sources; // actual wrapped sources
    private TaskDataSource currentSource;
    private SourceTracker tracker;

    public TaskDataSourceAggregator(Collection<TaskDataSource> sourceList, TaskDataSourceSelector sourceSelector,
                                    SourceTracker tracker) {
        if (sourceList == null || sourceSelector == null) {
            throw new NullPointerException();
        }

        this.sourceSelector = sourceSelector;
        this.tracker = tracker;
        this.sources = new HashMap<>();
        this.sourceList = new LinkedList<>(sourceList);

        for (TaskDataSource taskDataSource : sourceList) {
            if (this.tracker != null) {
                TaskDataSource tracked = tracker.openAndInit(taskDataSource);
                this.sources.put(taskDataSource, (tracked != null) ? tracked : taskDataSource);
            } else {
                this.sources.put(taskDataSource, taskDataSource);
            }
        }

        getNextSource();
    }

    private TaskDataSource selectNextSource() {
        return sourceSelector.selectSource(sourceList, currentSource);
    }

    @Override public void init(TaskRunConfig config) {
        // nothing to do
    }

    @Override
    public Bundle next() {
        return peek() != null ? currentSource.next() : null;
    }

    @Override
    public Bundle peek() {
        getNextSource();
        if (currentSource != null) {
            Bundle peek = currentSource.peek();
            if (peek != null) {
                return peek;
            }
        }
        // sources exhausted ... enumerate and log all used sources
        {
            StringBuilder sb = new StringBuilder();
            int len = sources.values().size();
            sb.append("All (" + len + ") sources have been closed from sourceList:\n\n");
            for (TaskDataSource taskDataSource : sources.values()) {
                sb.append("\t").append(taskDataSource).append("\n");
            }
            log.warn(sb.toString());
        }
        return null;
    }

    private void getNextSource() {
        TaskDataSource nextSource = selectNextSource();
        if (currentSource != nextSource) {
            currentSource = nextSource;
        }
    }

    @Override
    public void close() {
        for (Entry<TaskDataSource, TaskDataSource> entry : sources.entrySet()) {
            entry.getValue().close();
        }
        if (tracker != null) {
            tracker.close();
        }
    }
}
