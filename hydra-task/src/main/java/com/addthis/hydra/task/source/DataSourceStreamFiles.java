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

import com.addthis.basis.util.Strings;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.task.stream.StreamFileSource;
import com.addthis.hydra.task.stream.StreamSourceFileList;


/**
 * This data source <span class="hydra-summary">retrieves files from the local filesystem (for testing purposes)</span>.
 *
 * @user-reference
 * @hydra-name files
 */
public class DataSourceStreamFiles extends DataSourceStreamList {

    /**
     * List of files to read.
     */
    @FieldConfig(codable = true)
    private String[] files;

    @Override
    public StreamFileSource getSourceList(Integer[] shard) {
        return new StreamSourceFileList(Strings.join(files, "\n"));
    }
}
