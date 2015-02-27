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

/**
 * This {@link TaskDataSource source} <span class="hydra-summary">retrieves a subset of an underlying data source</span>.
 * <p>The first N bundles can be skipped using the 'skip' parameter. The source will return at most M bundles
 * using the 'limit' parameter.
 * </p>
 *
 * @user-reference
 * @hydra-name range
 */
public class DataSourceRange extends TaskDataSource {

    /**
     * Underlying data source. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private TaskDataSource source;

    /**
     * If non-zero then skip this number of bundles. Default is zero.
     */
    @FieldConfig(codable = true)
    private long skip;

    /**
     * If non-zero then return at most this number of bundles. Default is zero.
     */
    @FieldConfig(codable = true)
    private long limit;

    @Override
    public Bundle next() throws DataChannelError {
        while (skip-- > 0 && source.next() != null) {
            ;
        }
        return limit-- > 0 ? source.next() : null;
    }

    @Override
    public Bundle peek() throws DataChannelError {
        while (skip-- > 0 && source.next() != null) {
            ;
        }
        return limit > 0 ? source.peek() : null;
    }

    @Override
    public void close() {
        source.close();
    }

    @Override public void init() {
        source.init();
    }

    @Nonnull @Override public ImmutableList<String> outputRootDirs() {
        return source.outputRootDirs();
    }

}
