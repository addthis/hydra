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

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

public interface OutputRootDirectories {

    /**
     * List of root directories that are created. Some data sources
     * write persistent state to a directory in addition to
     * reading from other sources. Each implementing
     * class is responsible for specifying an URL scheme when
     * not referring to a path on the filesystem (ie. hdfs:// for example).
     */
    public default @Nonnull ImmutableList<String> outputRootDirs() {
        return ImmutableList.of();
    }

}
