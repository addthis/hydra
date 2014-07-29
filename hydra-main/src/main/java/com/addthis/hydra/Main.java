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
package com.addthis.hydra;

import java.io.File;

import java.lang.reflect.Method;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.plugins.PluginMap;
import com.addthis.codec.plugins.PluginRegistry;
import com.addthis.hydra.task.run.JsonRunner;
import com.addthis.metrics.reporter.config.ReporterConfig;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import com.yammer.metrics.reporting.GangliaReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * command-line/jar entry-point to start either hydra or batch.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final boolean GANGLIA_ENABLE = Parameter.boolValue("ganglia.enable", true);
    private static final String GANGLIA_HOSTS = Parameter.value("ganglia.hosts", "localhost:8649");
    private static final String METRICS_REPORTER_CONFIG_FILE = Parameter.value("metrics.reporter.config", "");
    private static final boolean GANGLIA_SHORT_NAMES = Parameter.boolValue("ganglia.useShortNames", false);

    public static void main(String[] args) {
        if (args.length > 0) {
            if (GANGLIA_ENABLE) {
                if (!Strings.isEmpty(METRICS_REPORTER_CONFIG_FILE)) {
                    log.info("enabling ganglia using reporter config: " + METRICS_REPORTER_CONFIG_FILE);
                    try {
                        ReporterConfig.loadFromFileAndValidate(METRICS_REPORTER_CONFIG_FILE).enableAll();
                    } catch (Exception e) {
                        log.warn("Failed to enable metrics reporter from file", e);
                        }
                } else {
                    log.info("enabling ganglia using host/ports: " + GANGLIA_HOSTS);
                    String[] splitHosts = GANGLIA_HOSTS.split(",");
                    for (String hostAndPort : splitHosts) {
                        String[] params = hostAndPort.split(":");
                        String host = params[0];
                        int port = Integer.parseInt(params[1]);
                        GangliaReporter.enable(60, TimeUnit.SECONDS, host, port, GANGLIA_SHORT_NAMES);
                    }
                }
            }
            try {
                switch(args[0]) {
                    /**
                     * For backwards compatibility the targets "validate", "zk",
                     * "dbspace", "mss", and "json" do not follow the standard format.
                     *
                     * Use the "-executables.classmap" plugin for new targets.
                     */
                    case "validate":
                        try {
                            JsonRunner.loadConfig(new File(args[1]));
                            System.out.println("task config is valid");
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        break;
                    case "zk": {
                        Class zk = Class.forName("org.apache.zookeeper.ZooKeeperMain");
                        Method m = zk.getDeclaredMethod("main", String[].class);
                        m.invoke(null, new Object[]{cutargs(args)});
                        break;
                    }
                    case "dbspace":
                        com.sleepycat.je.util.DbSpace.main(cutargs(args));
                        break;
                    case "mss":
                        String mssRoot = Parameter.value("mss.root", "streams");
                        String meshyPorts = Parameter.value("mss.mesh.ports", "5000");
                        String meshyPeers = Parameter.value("mss.mesh.peers", "");
                        for (String portGroup : Strings.splitArray(meshyPorts, ";")) {
                            System.out.println("[mss] starting meshy with port group: " + portGroup);
                            com.addthis.meshy.Main.main(new String[]{"server", portGroup, mssRoot, meshyPeers});
                        }
                        break;
                    default:
                        Class clazz = PluginRegistry.defaultRegistry().asMap()
                                                    .get("executables").asBiMap()
                                                    .get(args[0]);
                        if (clazz != null) {
                            Method m = clazz.getDeclaredMethod("main", String[].class);
                            m.invoke(null, (Object) cutargs(args));
                        } else {
                            usage();
                        }
                }
            } catch (Exception e) {
                System.err.println("Error starting process.");
                e.printStackTrace();
                System.exit(1);
            }
        } else {
            usage();
        }
    }

    private static String[] cutargs(String[] args) {
        String[] ns = new String[args.length - 1];
        System.arraycopy(args, 1, ns, 0, args.length - 1);
        return ns;
    }

    private static void usage() {
        PluginMap pluginMap = PluginRegistry.defaultRegistry().asMap().get("executables");
        Set<String> hardCodedOptions = Sets.newHashSet("validate", "zk", "dbspace", "mss");
        String options = Joiner.on(" | ").join(Sets.union(pluginMap.asBiMap().keySet(), hardCodedOptions));
        String usage = "usage: run [ " + options + " ]";
        System.out.println(usage);
    }
}
