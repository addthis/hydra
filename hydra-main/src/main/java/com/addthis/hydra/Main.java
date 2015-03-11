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

import java.lang.reflect.Method;

import java.util.Set;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.LessStrings;

import com.addthis.codec.config.Configs;
import com.addthis.codec.plugins.PluginMap;
import com.addthis.codec.plugins.PluginRegistry;
import com.addthis.hydra.common.util.CloseTask;
import com.addthis.hydra.util.BundleReporter;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

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
                    BundleReporter bundleReporter = Configs.decodeObject(
                            BundleReporter.class, globalConfig.getConfig("hydra.metrics.config"));
                    bundleReporter.start();
                    Runtime.getRuntime().addShutdownHook(
                            new Thread(new CloseTask(bundleReporter), "Metrics Bundle Reporter Shutdown Hook"));
                } catch (Exception e) {
                    log.warn("Failed to enable metrics reporter from config", e);
                }
            }
            try {
                switch(args[0]) {
                    case "mss":
                        String mssRoot = Parameter.value("mss.root", "streams");
                        String meshyPorts = Parameter.value("mss.mesh.ports", "5000");
                        String meshyPeers = Parameter.value("mss.mesh.peers", "");
                        for (String portGroup : LessStrings.splitArray(meshyPorts, ";")) {
                            log.info("[mss] starting meshy with port group: " + portGroup);
                            com.addthis.meshy.Main.main(new String[]{"server", portGroup, mssRoot, meshyPeers});
                        }
                        break;
                    default:
                        PluginMap executables = PluginRegistry.defaultRegistry().asMap().get("executables");
                        String name = args[0];
                        Class clazz = executables.asBiMap().get(name);
                        if (clazz != null) {
                            boolean showBanner = executables.config().getBoolean("_show-banner");
                            ConfigObject banners = executables.config().getObject("_banners");
                            if (showBanner && banners.containsKey(name)) {
                                String banner = banners.get(name).unwrapped().toString();
                                log.info(String.format("Starting {}%n{}"), name, banner);
                            }
                            Method m = clazz.getDeclaredMethod("main", String[].class);
                            m.invoke(null, (Object) cutargs(args));
                        } else {
                            usage();
                        }
                }
            } catch (Exception e) {
                log.error("Error starting process.", e);
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
