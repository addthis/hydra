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

import java.nio.file.Path;

import com.addthis.bundle.core.Bundle;
import com.addthis.hydra.data.filter.bundle.BundleFilter;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This data source <span class="hydra-summary">applies a filter to an input source</span>.
 * <p>
 * Example:
 * <p>
 * <pre>
 *  source.filter {
 *      filter.concat {in:["YMD", "HMS"], out:"TIME", join:" "}
 *      stream.mesh2 {
 *          mesh {...}
 *      }
 *  }
 *  </pre>
 *
 * @user-reference
 */
public class DataSourceFiltered extends TaskDataSource {

    /** Underlying data source from which data is fetched. */
    @JsonProperty(required = true) private TaskDataSource stream;

    /** Apply this filter to each bundle that is retrieved to the data source. */
    @JsonProperty(required = true) private BundleFilter filter;

    private Bundle peek;

    @Override
    public void init() {
        stream.init();
    }

    @Override
    public void close() {
        stream.close();
    }

    @Override
    public Bundle peek() {
        Bundle tmp = null;
        while (peek == null && (tmp = stream.peek()) != null) {
            if (!filter.filter(tmp)) {
                stream.next();
                continue;
            }
            peek = tmp;
        }
        return peek;
    }

    @Override
    public Bundle next() {
        Bundle filtered = peek();
        if (filtered == null) {
            return null;
        }
        peek = null;
        // peek might not be exactly equal to what we return here due to mutating filters on peek and other factors
        return stream.next();
    }

    @Nonnull @Override
    public ImmutableList<Path> writableRootPaths() {
        return stream.writableRootPaths();
    }
}
