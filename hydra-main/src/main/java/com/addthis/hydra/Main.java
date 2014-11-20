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
import java.io.IOException;
import java.lang.reflect.Method;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.util.Parameter;
import com.addthis.basis.util.Strings;

import com.addthis.codec.config.Configs;
import com.addthis.codec.plugins.PluginMap;
import com.addthis.codec.plugins.PluginRegistry;
import com.addthis.hydra.common.util.CloseTask;
import com.addthis.hydra.discovery.MesosServiceDiscoveryUtility;
import com.addthis.hydra.util.BundleReporter;

import com.addthis.meshy.MeshyServer;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * command-line/jar entry-point to start either hydra or batch.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
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
                        String meshyAppName = Parameter.value("mss.appName");
                        int portIndex = Parameter.intValue("mss.portIndex", 0);
                        int peerInterval = Parameter.intValue("mss.peerInterval", 15);

                        // TODO:  this only supports one port and default network interface
                        int portNum = Integer.parseInt(meshyPorts);
                        MeshyServer meshy = new MeshyServer(portNum, new File(mssRoot));

                        if (meshyPeers != null && !meshyPeers.isEmpty()) {
                            for (String peer : Strings.splitArray(meshyPeers, ",")) {
                                String hostPort[] = Strings.splitArray(peer, ":");
                                int port = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : meshy.getLocalAddress().getPort();
                                meshy.connectPeer(new InetSocketAddress(hostPort[0], port));
                            }
                        }

                        if (meshyAppName != null) {
                            ScheduledExecutorService queryMeshPeerMaintainer = MoreExecutors
                                    .getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1,
                                            new ThreadFactoryBuilder().setNameFormat("meshPeerMaintainer=%d").build()));
                            queryMeshPeerMaintainer.scheduleAtFixedRate(() -> {
                                Optional<Map<String, Integer>> peerMap = MesosServiceDiscoveryUtility.getTaskHosts(meshyAppName, portIndex);
                                if (peerMap.isPresent()) {
                                    for (Map.Entry<String, Integer> entry : peerMap.get().entrySet()) {
                                        log.debug(String.format("mesh node peering with %s : %d", entry.getKey(), entry.getValue()));
                                        ChannelFuture future = meshy.connectToPeer(entry.getKey(), new InetSocketAddress(entry.getKey(), entry.getValue()));
                                        if (future == null) {
                                            if (log.isDebugEnabled()) {
                                                // means we've already connected
                                                log.debug("Meshy peer connect returned null future to " + new InetSocketAddress(entry.getKey(), entry.getValue()));
                                            }
                                            continue;
                                        }
                                        /* wait for connection to complete */
                                        future.awaitUninterruptibly();
                                        if (!future.isSuccess()) {
                                            meshy.dropPeer(entry.getKey());
                                            log.warn("Meshy peer connect fail to " + new InetSocketAddress(entry.getKey(), entry.getValue()));
                                        }
                                    }
                                }
                            }, 0, peerInterval, TimeUnit.SECONDS);
                        }
                        break;
                    default:
                        PluginMap executables = PluginRegistry.defaultRegistry().asMap().get("executables");
                        String name = args[0];
                        Class clazz = executables.asBiMap().get(name);
                        if (clazz != null) {
                            ConfigObject banners = executables.config().getObject("_banners");
                            if (banners.containsKey(name)) {
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
