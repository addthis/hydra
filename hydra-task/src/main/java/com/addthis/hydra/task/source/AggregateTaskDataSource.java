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

import java.util.LinkedList;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.task.run.TaskRunConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This data source <span class="hydra-summary">aggregates data from one or more data sources</span>.
 * <p/>
 * <p>The user specifies an ordered sequence of data sources. The aggregate
 * data source first retrieves all the data from the first source. When the first
 * source has been exhausted, then the data is retrieved from the
 * second source. And so on and so forth until all the data has been retrieved.
 * <p/>
 * <p><b>CAUTION:</b> If the individual data sources have mark directories
 * then you must set these mark directories to be different locations for
 * each data source. Otherwise the mark information will end up in
 * an inconsistent state. The data team is working on enforcing this requirement
 * automatically.</p>
 * <p/>
 * <p>Example:</p>
 * <pre>source : {
 *   type :  "aggregate",
 *   sources : [
 *     {
 *       type : "stream2",
 *       hash : true,
 *       markDir : "markSource1",
 *       source: {
 *         ...
 *       },
 *       factory: {
 *         ...
 *       }
 *     },
 *     {
 *       type : "mesh2",
 *       markDir : "markSource2",
 *       mesh : {
 *         ...
 *       },
 *       factory : {
 *         ...
 *       },
 *     }
 *   ]
 * },</pre>
 *
 * @user-reference
 * @hydra-name aggregate
 */
public class AggregateTaskDataSource extends TaskDataSource {

    private static final Logger log = LoggerFactory.getLogger(AggregateTaskDataSource.class);

    /**
     * an ordered sequence of data sources. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private TaskDataSource[] sources;

    private TaskDataSource currentSource;

    private final LinkedList<TaskDataSource> sourceList = new LinkedList<>();

    // to support test cases
    protected void setSources(TaskDataSource[] sources) {
        this.sources = sources;
    }

    @Override
    public boolean hadMoreData() {
        for (TaskDataSource source : sources) {
            if (source.hadMoreData()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void init(TaskRunConfig config) {
        for (TaskDataSource source : sources) {
            if (source.isEnabled()) {
                log.debug("init {}", source);
                source.init(config);
                sourceList.add(source);
            } else {
                log.debug("disabled {}", source);
            }
        }
        requireValidSource();
    }

    @Override
    public void close() {
        for (TaskDataSource source : sources) {
            if ((source != null) && source.isEnabled()) {
                log.debug("close {}", source);
                source.close();
            }
        }
    }

    private void resetCurrentSource() {
        log.debug("resetCurrentSource {}", currentSource);
        currentSource = null;
    }

    private boolean requireValidSource() {
        while ((currentSource == null) && !sourceList.isEmpty()) {
            currentSource = sourceList.removeFirst();
            log.debug("nextSource = {}", currentSource);
            if (currentSource.peek() != null) {
                log.debug("setSource {}", currentSource);
                return true;
            }
            currentSource = null;
        }
        return currentSource != null;
    }

    @Override
    public Bundle next() throws DataChannelError {
        while (requireValidSource()) {
            Bundle next = currentSource.next();
            log.debug("next {}", next);
            if (next != null) {
                return next;
            }
            resetCurrentSource();
        }
        return null;
    }

    @Override
    public Bundle peek() throws DataChannelError {
        while (requireValidSource()) {
            Bundle peek = currentSource.peek();
            log.debug("peek {}", peek);
            if (peek != null) {
                return peek;
            }
            resetCurrentSource();
        }
        return null;
    }
}
