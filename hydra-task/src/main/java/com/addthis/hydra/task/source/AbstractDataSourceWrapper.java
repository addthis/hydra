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

import javax.annotation.Nonnull;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;

import com.google.common.collect.ImmutableList;


public abstract class AbstractDataSourceWrapper extends TaskDataSource {

    @FieldConfig(codable = true, required = true)
    private TaskDataSource source;

    public AbstractDataSourceWrapper() {
    }

    public TaskDataSource getSource() {
        return source;
    }

    @Override
    public String toString() {
        return source.toString();
    }

    public AbstractDataSourceWrapper(TaskDataSource source) {
        this.source = source;
    }

    @Override
    public void init() throws DataChannelError {
        source.init();
    }

    @Override
    public Bundle next() throws DataChannelError {
        return source.next();
    }

    @Override
    public Bundle peek() throws DataChannelError {
        return source.peek();
    }

    @Nonnull @Override public ImmutableList<String> outputRootDirs() {
        return source.outputRootDirs();
    }

    @Override
    public void close() {
        source.close();
    }
}
