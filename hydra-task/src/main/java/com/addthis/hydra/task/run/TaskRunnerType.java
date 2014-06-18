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
