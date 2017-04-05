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
package com.addthis.hydra.job.alert;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holds the user group alert configs. Necessary to convert from a <code>Collection&lt;Group&gt;</code>
 * to a <code>Map&lt;String, Group&gt;</code>
 */
public class GroupManager {

    private Map<String, Group> groups;

    @JsonCreator public GroupManager(@JsonProperty(value = "groups") Collection<Group> groups) {
        this.groups = groups.stream().collect(Collectors.toMap(group -> group.name, Function.identity()));
    }

    public Group getGroup(String name) {
        return groups.get(name);
    }
}
