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

package com.addthis.hydra.query.aggregate;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;

public class TaskSourceOptionInfo implements Codable {

    @FieldConfig(codable = true)
    public final String hostUuid;
    @FieldConfig(codable = true)
    public final boolean active;
    @FieldConfig(codable = true)
    public final boolean selected;

    public TaskSourceOptionInfo(QueryTaskSourceOption option, boolean selected) {
        this.hostUuid = option.queryReference.getHostUUID();
        this.active   = option.isActive();
        this.selected = selected;
    }
}
