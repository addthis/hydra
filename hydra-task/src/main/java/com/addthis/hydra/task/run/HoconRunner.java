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
import java.io.IOException;

import com.addthis.basis.util.Parameter;

import com.addthis.codec.jackson.CodecJackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigResolveOptions;

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

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: hocon <config> <nodes> <node> [jobid] [threads]");
            return;
        }
        String fileName = args[0];
        Config config = ConfigFactory.parseFile(new File(fileName));
        runTask(config, args);
    }

    /**
     * Creates a TaskRunnable using CodecConfig and a little custom handling. At the root
     * level object, if there is a field named "global", then that sub tree is hoisted
     * up to override system properties (removing it from the root of the job config).
     * Either way, the job config will also then be resolved against config defaults/ system
     * properties for the purposes of variable substitution. This will not merge them entirely
     * though and so the job config will be otherwise unaffected.
     */
    public static TaskRunnable makeTask(Config config) throws JsonProcessingException, IOException {
        Config jobConfig = config;
        CodecJackson codec;
        if (config.hasPath("global")) {
            jobConfig = config.withoutPath("global").resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
            Config globalDefaults = config.getConfig("global")
                                          .withFallback(ConfigFactory.load())
                                          .resolve();
            jobConfig = jobConfig.resolveWith(globalDefaults);
            codec = CodecJackson.getDefault().withConfig(globalDefaults);
        } else {
            jobConfig = jobConfig.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                                 .resolveWith(ConfigFactory.load());
            codec = CodecJackson.getDefault();
        }
        return codec.decodeObject(TaskRunnable.class, jobConfig);
    }

    static void runTask(Config config, String[] args) throws Exception {
        if (!JsonRunner.checkArgs(args)) return;
        int nodeCount = Integer.parseInt(args[1]);
        int thisNode = Integer.parseInt(args[2]);
        String jobId = (args.length > 3) ? args[3] : null;
        runTask(config, nodeCount, thisNode, jobId);
    }

    static void runTask(String configString, int nodeCount, int thisNode,
                        String jobId) throws Exception {
        Config config = ConfigFactory.parseString(configString);
        runTask(config, nodeCount, thisNode, jobId);
    }

    static void runTask(Config config, int nodeCount, int thisNode,
                        String jobId) throws Exception {

        final TaskRunnable task = makeTask(config);
        TaskRunConfig taskRunConfig = new TaskRunConfig(thisNode, nodeCount, jobId);
        task.init(taskRunConfig);
        task.exec();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                task.terminate();
                task.waitExit();
            }
        });
    }

    static void runCompatTask(String config, String[] args) throws Exception {
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
