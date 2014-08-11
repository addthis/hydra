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

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.config.Configs;
import com.addthis.codec.plugins.PluginMap;
import com.addthis.codec.plugins.PluginRegistry;
import com.addthis.hydra.task.run.JsonRunner;
import com.addthis.metrics.reporter.config.ReporterConfig;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * command-line/jar entry-point to start either hydra or batch.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length > 0) {
            Config globalConfig = ConfigFactory.load();
            if (globalConfig.getBoolean("hydra.metrics.enable")) {
                try {
                    Configs.decodeObject(ReporterConfig.class,
                                         globalConfig.getConfig("hydra.metrics.config"))
                           .enableAll();
                } catch (Exception e) {
                    log.warn("Failed to enable metrics reporter from config", e);
                }
            }
            try {
                switch(args[0]) {
                    case "validate":
                        try {
                            JsonRunner.loadConfig(new File(args[1]));
                            System.out.println("task config is valid");
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
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
        Set<String> hardCodedOptions = Sets.newHashSet("validate", "mss");
        String options = Joiner.on(" | ").join(Sets.union(pluginMap.asBiMap().keySet(), hardCodedOptions));
        String usage = "usage: run [ " + options + " ]";
        System.out.println(usage);
    }
}
