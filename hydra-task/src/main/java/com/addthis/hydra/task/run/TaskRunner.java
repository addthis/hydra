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
import java.util.concurrent.CompletableFuture;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;

import com.addthis.codec.jackson.CodecJackson;
import com.addthis.codec.jackson.Jackson;
import com.addthis.muxy.MuxFileDirectoryCache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskRunner {
    private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: task <config> <nodes> <node> [jobid] [threads]");
            return;
        }
        String fileName = args[0];
        String configString = loadStringFromFile(fileName);
        runTask(configString);
    }

    static void runTask(String configString) throws Exception {
        final TaskRunnable task = makeTask(configString);
        // before starting, we need to make sure that the task will be closed
        CompletableFuture<AutoCloseable> startedTask = new CompletableFuture<>();
        boolean hookAdded;
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(new TaskShutdown(startedTask),
                                                            "Task Shutdown Hook"));
            hookAdded = true;
        } catch (IllegalStateException ignored) {
            log.info("Canceling task because JVM is shutting down.");
            hookAdded = false;
        }
        if (hookAdded) {
            try {
                task.start();
                startedTask.complete(task);
            } catch (Throwable t) {
                startedTask.complete(() -> log.debug("skipping task.close because it failed to start normally"));
                throw t;
            }
        }
    }

    private static class TaskShutdown implements Runnable {

        private final CompletableFuture<AutoCloseable> task;

        TaskShutdown(CompletableFuture<AutoCloseable> task) {
            this.task = task;
        }

        @Override public void run() {
            try {
                task.join().close();
                // critical to get any file meta data written before process exits
                CompletableFuture.runAsync(MuxFileDirectoryCache::waitForWriteClosure).join();
            } catch (Exception ex) {
                log.error("unrecoverable error shutting down task. immediately halting jvm", ex);
                Runtime.getRuntime().halt(1);
            }
        }
    }

    public static TaskRunnable makeTask(String configString) throws JsonProcessingException, IOException {
        return makeTask(configString, Jackson.defaultCodec());
    }

    /**
     * Creates a TaskRunnable using CodecConfig and a little custom handling. At the root
     * level object, if there is a field named "global", then that sub tree is hoisted
     * up to override system properties (removing it from the root of the job config).
     * Either way, the job config will also then be resolved against config defaults/ system
     * properties for the purposes of variable substitution. This will not merge them entirely
     * though and so the job config will be otherwise unaffected.
     */
    public static TaskRunnable makeTask(String configString, CodecJackson defaultCodec)
            throws JsonProcessingException, IOException {
        String subbedConfigString = subAt(configString);
        Config config = ConfigFactory.parseString(subbedConfigString,
                                                  ConfigParseOptions.defaults().setOriginDescription("job.conf"));
        Config defaultGlobalDefaults = defaultCodec.getGlobalDefaults();
        Config jobConfig = config;
        CodecJackson codec;
        if (config.root().containsKey("global")) {
            jobConfig = config.root().withoutKey("global").toConfig()
                              .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
            Config globalDefaults = config.getConfig("global")
                                          .withFallback(defaultGlobalDefaults)
                                          .resolve();
            jobConfig = jobConfig.resolveWith(globalDefaults);
            codec = defaultCodec.withConfig(globalDefaults);
        } else {
            jobConfig = jobConfig.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                                 .resolveWith(defaultGlobalDefaults);
            codec = defaultCodec;
        }
        return codec.decodeObject(TaskRunnable.class, jobConfig);
    }

    static String loadStringFromFile(String fileName) throws IOException {
        return LessBytes.toString(LessFiles.read(new File(fileName)));
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
