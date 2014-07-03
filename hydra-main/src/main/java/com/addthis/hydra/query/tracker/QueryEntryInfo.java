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

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.hydra.query.aggregate.TaskSourceInfo;

public class QueryEntryInfo implements Codable {

    @FieldConfig(codable = true)
    public String[] paths;
    @FieldConfig(codable = true)
    public long uuid;
    @FieldConfig(codable = true)
    public String alias;
    @FieldConfig(codable = true)
    public String sources;
    @FieldConfig(codable = true)
    public String remoteip;
    @FieldConfig(codable = true)
    public String sender;
    @FieldConfig(codable = true)
    public String job;
    @FieldConfig(codable = true)
    public String[] ops;
    @FieldConfig(codable = true)
    public long startTime;
    @FieldConfig(codable = true)
    public long runTime;
    @FieldConfig(codable = true)
    public long lines;
    @FieldConfig(codable = true)
    public long sentLines;
    @FieldConfig(codable = true)
    public QueryState state;
    @FieldConfig(codable = true)
    public TaskSourceInfo[] tasks;
}
