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

import java.io.File;

import com.addthis.basis.util.Parameter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

/**
 * Main method invoked when running tasks.
 * <p/>
 * "usage: hocon <config> <nodes> <node> [jobid] [threads]"
 */
public class HoconRunner {

    static final boolean resolvePrint = Parameter.boolValue("task.hocon.resolvePrint", false);
    static final boolean toJson = Parameter.boolValue("task.hocon.toJson", false);
    static final boolean keepComments = Parameter.boolValue("task.hocon.keepComments", false);
    static final boolean debugComments = Parameter.boolValue("task.hocon.debugComments", false);

    public static void main(String args[]) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: hocon <config> <nodes> <node> [jobid] [threads]");
            return;
        }
        String fileName = args[0];
        String configString = TaskRunner.loadStringFromFile(fileName);
        if (args.length < 2) {
            loadHoconAndPrintVarious(configString);
        } else {
            runTask(configString, args);
        }
    }

    static void runTask(String config, String[] args) throws Exception {
        String json = loadHoconAndPrintJson(config);
        JsonRunner.runTask(json, args);
    }

    /**
     * Convert a hocon formatted string into a json formatted string.
     *
     * @param configString hocon formatted string.
     * @return json formatted string
     */
    static String loadHoconAndPrintJson(String configString) {
        Config config = ConfigFactory.parseString(configString);
        return config.resolve().root().render(ConfigRenderOptions.concise());
    }

    /**
     * Convert a hocon formatted string into a json formatted string
     * and print the json string to standard output.
     *
     * @param configString hocon formatted string.
     */
    static void loadHoconAndPrintVarious(String configString) {
        Config config = ConfigFactory.parseString(configString);
        if (resolvePrint) {
            config = config.resolve();
        }
        ConfigRenderOptions renderOptions = ConfigRenderOptions.defaults()
                .setComments(keepComments)
                .setOriginComments(debugComments)
                .setJson(toJson);
        String outputString = config.root().render(renderOptions);
        System.out.println(outputString);
    }
}
