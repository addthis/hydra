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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.Codec;
import com.addthis.hydra.common.plugins.PluginReader;
import com.addthis.hydra.data.filter.bundle.BundleFilter;
import com.addthis.hydra.data.query.CLIQuery;
import com.addthis.hydra.job.Minion;
import com.addthis.hydra.job.Spawn;
import com.addthis.hydra.query.MeshQueryWorker;
import com.addthis.hydra.query.QueryServer;
import com.addthis.hydra.query.util.QueryChannelUtil;
import com.addthis.hydra.task.run.TaskRunner;
import com.addthis.hydra.task.util.BundleStreamPeeker;
import com.addthis.maljson.JSONObject;
import com.addthis.metrics.reporter.config.ReporterConfig;

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

    public static final Map<String, Class> cmap = new HashMap<>();

    @SuppressWarnings("unused")
    public static void registerFilter(String name, Class clazz) {
        cmap.put(name, clazz);
    }

    /** register types */
    static {
        PluginReader.registerPlugin("-executables.classmap", cmap, Object.class);
    }


    public static void main(String args[]) {
        initLog4j();
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
                            TaskRunner.loadConfig(new File(args[1]));
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
                    case "json":
                        JSONObject j = new JSONObject(args[1]);
                        System.out.println(j.toString());
                        break;
                    default:
                        Class clazz = cmap.get(args[0]);
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

    private static String getHostName() {
        String hostName = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            // Get hostname
            hostName = addr.getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return hostName;
    }

    private static String[] cutargs(String args[]) {
        String[] ns = new String[args.length - 1];
        System.arraycopy(args, 1, ns, 0, args.length - 1);
        return ns;
    }

    private static void usage() {
        System.out.println(
                "usage: run [ spawn | minion | qmaster | qworker | qutil | streamer | task | fmux | cliquery | printbundles | mesh | mss | zk | dbspace ] <args>"
        );
    }


    protected static boolean isClassAvailable(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    // Addapted from CassandraDaemon:initLog4j
    public static void initLog4j()
    {
        if (!isClassAvailable("org.apache.log4j.PropertyConfigurator")) {
            System.err.println("org.apache.log4j.PropertyConfigurator not found, can not enable log4j logging");
            return;
        }

        if (System.getProperty("log4j.defaultInitOverride","false").equalsIgnoreCase("true"))  {
            String config = System.getProperty("log4j.configuration", "log4j.properties");
            if (config.startsWith("/")) {
                config = "file://" + config;
            }
            URL configLocation = null;
            try {
                // try loading from a physical location first.
                configLocation = new URL(config);
            } catch (MalformedURLException ex) {
                // then try loading from the classpath.
                configLocation = Main.class.getClassLoader().getResource(config);
            }

            if (configLocation == null) {
                throw new RuntimeException("Couldn't figure out log4j configuration: "+config);
            }

            // Now convert URL to a filename
            String configFileName = null;
            try {
                // first try URL.getFile() which works for opaque URLs (file:foo) and paths without spaces
                configFileName = configLocation.getFile();
                File configFile = new File(configFileName);
                // then try alternative approach which works for all hierarchical URLs with or without spaces
                if (!configFile.exists()) {
                    configFileName = new File(configLocation.toURI()).getPath();
                }
            } catch (Exception e) {
                throw new RuntimeException("Couldn't convert log4j configuration location to a valid file", e);
            }

            try {
                Class.forName("org.apache.log4j.PropertyConfigurator").getMethod("configureAndWatch", String.class, Long.TYPE).invoke(null, configFileName, 10000);
            } catch(Exception e) {
                throw new RuntimeException("Failed to enable log4j", e);
            }
            log.info("log4j initialized");
        }
        else {
            System.err.println("Explicitly asked for log4j initialization, but had log4j.defaultInitOverride!=true. Default log4j properties may or may not be used");
        }
    }

}
