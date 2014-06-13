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

package com.addthis.hydra.query.tracker;

import com.addthis.codec.Codec;
import com.addthis.hydra.query.aggregate.TaskSourceInfo;

public class QueryEntryInfo implements Codec.Codable {

    @Codec.Set(codable = true)
    public String[] paths;
    @Codec.Set(codable = true)
    public long uuid;
    @Codec.Set(codable = true)
    public String alias;
    @Codec.Set(codable = true)
    public String sources;
    @Codec.Set(codable = true)
    public String remoteip;
    @Codec.Set(codable = true)
    public String sender;
    @Codec.Set(codable = true)
    public String job;
    @Codec.Set(codable = true)
    public String[] ops;
    @Codec.Set(codable = true)
    public long startTime;
    @Codec.Set(codable = true)
    public long runTime;
    @Codec.Set(codable = true)
    public long lines;
    @Codec.Set(codable = true)
    public long sentLines;
    @Codec.Set(codable = true)
    public QueryState state;
    @Codec.Set(codable = true)
    public TaskSourceInfo[] tasks;
}
