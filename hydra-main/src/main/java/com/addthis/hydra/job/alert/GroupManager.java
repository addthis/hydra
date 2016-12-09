package com.addthis.hydra.job.alert;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GroupManager {

    private Map<String, Group> groups;

    @JsonCreator public GroupManager(@JsonProperty(value = "groups") Collection<Group> groups) {
        this.groups = groups.stream().collect(Collectors.toMap(group -> group.name, Function.identity()));
    }

    public Group getGroup(String name) {
        return groups.get(name);
    }
}
