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
package com.addthis.hydra.task.output.tree;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.data.tree.TreeCommonParameters;

public final class StoreConfig implements Codable {

    @FieldConfig(codable = true)
    Integer memSample;
    @FieldConfig(codable = true)
    Integer maxCacheSize;
    @FieldConfig(codable = true)
    Integer maxPageSize;
    @FieldConfig(codable = true)
    Long    maxCacheMem;
    @FieldConfig(codable = true)
    Integer maxPageMem;

    public void setStaticFieldsFromMembers() {
        if (maxCacheSize != null)
            TreeCommonParameters.setDefaultMaxCacheSize(maxCacheSize);
        if (maxCacheMem != null)
            TreeCommonParameters.setDefaultMaxCacheMem(maxCacheMem);
        if (maxPageSize != null)
            TreeCommonParameters.setDefaultMaxPageSize(maxCacheSize);
        if (maxPageMem != null)
            TreeCommonParameters.setDefaultMaxPageMem(maxPageMem);
        if (memSample != null)
            TreeCommonParameters.setDefaultMemSample(memSample);
    }
}
