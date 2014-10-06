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

import java.util.HashSet;
import java.util.Set;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;

import com.addthis.codec.jackson.CodecJackson;
import com.addthis.codec.jackson.Jackson;
import com.addthis.hydra.common.util.CloseTask;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;

public class TaskRunner {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: task <config> <nodes> <node> [jobid] [threads]");
            return;
        }
        String fileName = args[0];
        String configString = loadStringFromFile(fileName);
        runTask(configString, args);
    }

    static void runTask(String configString, String[] args) throws Exception {
        int nodeCount = Integer.parseInt(args[1]);
        int thisNode = Integer.parseInt(args[2]);
        String jobId = (args.length > 3) ? args[3] : null;
        runTask(configString, nodeCount, thisNode, jobId);
    }

    static void runTask(String configString, int nodeCount, int thisNode, String jobId) throws Exception {
        final TaskRunnable task = makeTask(configString);
        task.init();
        task.exec();

        Runtime.getRuntime().addShutdownHook(new Thread(new CloseTask(task), "Task Shutdown Hook"));
    }

    /**
     * Creates a TaskRunnable using CodecConfig and a little custom handling. At the root
     * level object, if there is a field named "global", then that sub tree is hoisted
     * up to override system properties (removing it from the root of the job config).
     * Either way, the job config will also then be resolved against config defaults/ system
     * properties for the purposes of variable substitution. This will not merge them entirely
     * though and so the job config will be otherwise unaffected.
     */
    public static TaskRunnable makeTask(String configString) throws JsonProcessingException, IOException {
        String subbedConfigString = subAt(configString);
        Config config = ConfigFactory.parseString(subbedConfigString,
                                                  ConfigParseOptions.defaults().setOriginDescription("job.conf"));
        Config jobConfig = config;
        CodecJackson codec;
        if (config.root().containsKey("global")) {
            jobConfig = config.root().withoutKey("global").toConfig()
                              .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
            Config globalDefaults = config.getConfig("global")
                                          .withFallback(ConfigFactory.load())
                                          .resolve();
            jobConfig = jobConfig.resolveWith(globalDefaults);
            codec = Jackson.defaultCodec().withConfig(globalDefaults);
        } else {
            jobConfig = jobConfig.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                                 .resolveWith(ConfigFactory.load());
            codec = Jackson.defaultCodec();
        }
        return codec.decodeObject(TaskRunnable.class, jobConfig);
    }

    static String loadStringFromFile(String fileName) throws IOException {
        return Bytes.toString(Files.read(new File(fileName)));
    }

    private static final Set<TaskStringReplacement> replaceOperators = new HashSet<>();

    static {
        replaceOperators.add(new TaskReplacementFile());
        replaceOperators.add(new TaskReplacementZoo());
    }

    /** replace references with file contents */
    static String subAt(String json) throws IOException {
        boolean transformed;
        String output = json;

        do {
            String begin = output;
            for(TaskStringReplacement replacement : replaceOperators) {
                output = replacement.replace(output);
            }
            transformed = (begin != output);
        } while(transformed);

        return output;
    }
}
