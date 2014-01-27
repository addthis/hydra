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
package com.addthis.hydra.job;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Map;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import com.addthis.codec.Codec;
import com.addthis.codec.CodecJSON;
import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyClientConnector;
import com.addthis.meshy.service.message.MessageFileProvider;
import com.addthis.meshy.service.message.MessageListener;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;
/**
 * Date: 10/22/12
 * Time: 5:23 PM
 * <p/>
 * Attach Spawn to Mesh and present an API / Filesystem for Minions and others
 */
public class SpawnMesh implements MessageListener {

    private static Logger log = LoggerFactory.getLogger(SpawnMesh.class);
    private static Codec codec = new CodecJSON();

    private static final String meshHost = Parameter.value("mesh.host", "localhost");
    private static final int meshPort = Parameter.intValue("mesh.port", 0);
    private static final int meshRetryTimeout = Parameter.intValue("mesh.retry.timeout", 5000);

    private final Spawn spawn;
    private final String meshPrefix;

    private MeshyClientConnector meshClient;
    private MessageFileProvider provider;

    SpawnMesh(final Spawn spawn) {
        this.spawn = spawn;
        this.meshPrefix = "/spawn/" + spawn.getUuid();
        if (meshPort == 0) {
            return;
        }
        meshClient = new MeshyClientConnector(meshHost, meshPort, 1000, meshRetryTimeout) {
            @Override
            public void linkUp(MeshyClient client) {
                log.warn("connected to mesh on " + client);
                provider = new MessageFileProvider(client);
                provider.setListener(meshPrefix + "/status", SpawnMesh.this);
                provider.setListener(meshPrefix + "/shutdown", SpawnMesh.this);
                provider.setListener(meshPrefix + "/task/get.config", SpawnMesh.this);
            }

            @Override
            public void linkDown(MeshyClient client) {
                log.warn("disconnected from mesh on " + client);
            }
        };
    }

    public MeshyClient getClient() {
        if (meshClient != null) {
            return meshClient.getClient();
        } else {
            return null;
        }
    }

    private String getString(Map<String, String> map, String key, String defVal) {
        try {
            String v = map.get(key);
            return v != null ? v : defVal;
        } catch (Exception ex) {
            return defVal;
        }
    }

    private int getInt(Map<String, String> map, String key, int defVal) {
        try {
            return Integer.parseInt(map.get(key));
        } catch (Exception ex) {
            return defVal;
        }
    }

    @Override
    public void requestContents(String fileName, Map<String, String> options, OutputStream out) throws IOException {
        if (fileName.endsWith("/status")) {
            send(out, spawn);
        } else if (fileName.endsWith("/shutdown")) {
            if (options != null && options.containsKey("yes")) {
                send(out, "shutting down");
                System.exit(1);
            } else {
                send(out, "ignored. send yes=1 to initiate shutdown");
            }
        } else if (fileName.endsWith("/job/list")) {
            // TODO
            send(out, "TODO");
        } else if (fileName.endsWith("/task/get.config")) {
            if (options != null && options.containsKey("job") && options.containsKey("task")) {
                Job job = spawn.getJob(options.get("job"));
                JobTask task = spawn.getTask(job.getId(), getInt(options, "task", -1));
                CommandTaskKick kick = spawn.getCommandTaskKick(job, task);
                if (kick == null) {
                    send(out, "{error:'no such job or task'}");
                } else {
                    send(out, kick);
                }
            } else {
                send(out, "{error:'missing job and/or task parameters'}");
            }
        }
    }

    private void send(OutputStream out, String msg) {
        try {
            out.write(Bytes.toBytes(msg));
            out.write('\n');
            out.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void send(OutputStream out, Codec.Codable msg) {
        try {
            out.write(codec.encode(msg));
            out.write('\n');
            out.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void send(OutputStream out, byte msg[]) {
        try {
            out.write(msg);
            out.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static String getMeshHost() {
        return meshHost;
    }

    public static int getMeshPort() {
        return meshPort;
    }
}
