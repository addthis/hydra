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

import com.addthis.codec.Codec;

public class TaskSourceInfo implements Codec.Codable {

    @Codec.Set(codable = true)
    public final boolean complete;
    @Codec.Set(codable = true)
    public final int lines;
    @Codec.Set(codable = true)
    public final long endTime;
    @Codec.Set(codable = true)
    public final TaskSourceOptionInfo[] options;

    public TaskSourceInfo(QueryTaskSource taskSource) {
        complete = taskSource.complete();
        lines    = taskSource.lines;
        endTime  = taskSource.endTime;
        options  = new TaskSourceOptionInfo[taskSource.options.length];
        for (int i = 0; i < taskSource.options.length; i++) {
            options[i] = new TaskSourceOptionInfo(taskSource.options[i]);
        }
    }

}
