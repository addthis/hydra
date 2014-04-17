package com.addthis.hydra.task.run;

import java.util.regex.Pattern;

public enum TaskRunnerType {
    JSON("(?!)"), // should never match
    HOCON("input\\s*:\\s*(hocon|com\\.typesafe\\.config)");

    private final Pattern pattern;

    TaskRunnerType(String regex) {
        this.pattern = Pattern.compile(regex);
    }

    public Pattern getPattern() {
        return pattern;
    }

    public static TaskRunnerType defaultType() {
        return JSON;
    }

    public void runTask(String config, String[] args) throws Exception {
        switch(this) {
            case JSON:
                JsonRunner.runTask(config, args);
                break;
            case HOCON:
                HoconRunner.runTask(config, args);
                break;
        }
    }
}
