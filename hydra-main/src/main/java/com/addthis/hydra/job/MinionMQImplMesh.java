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
 */package com.addthis.hydra.job;

import com.addthis.basis.util.Parameter;
import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.mq.MeshMessageConsumer;
import com.addthis.hydra.mq.MeshMessageProducer;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageListener;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.meshy.MeshyClient;
import com.addthis.meshy.MeshyClientConnector;
import com.rabbitmq.client.AlreadyClosedException;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mesh Messaging
 */
public class MinionMQImplMesh implements MinionMQ {
    private static Logger log = LoggerFactory.getLogger(MinionMQImplMesh.class);
    private static final String meshHost = Parameter.value("mesh.host", "localhost");
    private static final int meshPort = Parameter.intValue("mesh.port", 5000);
    private static final int meshRetryTimeout = Parameter.intValue("mesh.retry.timeout", 5000);

    private MeshyClientConnector mesh;
    private BlockingArrayQueue<HostMessage> queuedHostMessages;
    private MessageConsumer batchControlConsumer;
    private MessageProducer queryControlProducer;
    private MessageProducer batchControlProducer;

    @Override
    public void connect(final String uuid, final MessageListener listener) throws Exception {
        log.info("Queueing via Mesh");
        final AtomicBoolean up = new AtomicBoolean(false);
        mesh = new MeshyClientConnector(meshHost, meshPort, 1000, meshRetryTimeout) {
            @Override
            public void linkUp(MeshyClient client) {
                log.info("connected to mesh on {}", client.toString());
                up.set(true);
                synchronized (this) {
                    this.notify();
                }
            }

            @Override
            public void linkDown(MeshyClient client) {
                log.info("disconnected from mesh on {}", client.toString());
            }
        };
        while (!up.get()) {
            synchronized (mesh) {
                mesh.wait(1000);
            }
        }
        batchControlProducer = new MeshMessageProducer(mesh.getClient(), "CSBatchControl");
        queryControlProducer = new MeshMessageProducer(mesh.getClient(), "CSBatchQuery");
        queuedHostMessages = new BlockingArrayQueue<>();
        MeshMessageConsumer jobConsumer = new MeshMessageConsumer(mesh.getClient(), "CSBatchJob", uuid);
        jobConsumer.addRoutingKey(HostMessage.ALL_HOSTS);
        jobConsumer.addMessageListener(new MessageListener() {
            @Override
            public void onMessage(Serializable message) {
                try {
                    queuedHostMessages.put((HostMessage) message);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        });
        batchControlConsumer = new MeshMessageConsumer(mesh.getClient(), "CSBatchControl", uuid).addRoutingKey(HostMessage.ALL_HOSTS);
        batchControlConsumer.addMessageListener(listener);
    }

    public boolean sendControlMessage(HostMessage msg) {
        try {
            if (batchControlProducer != null) {
                batchControlProducer.sendMessage(msg, msg.getHostUuid());
                return true;
            }
        } catch (Exception ex) {
            log.warn("[mq.ctrl.send] fail with " + ex, ex);
        }
        return false;
    }

    public boolean sendStatusMessage(HostMessage msg) {
        try {
            if (batchControlProducer != null) {
                batchControlProducer.sendMessage(msg, "SPAWN");
                return true;
            }
        } catch (Exception ex) {
            log.warn("[mq.ctrl.send] fail with " + ex, ex);
        }
        return false;
    }

    public CommandTaskKick pollKickMessages() throws InterruptedException {
        try {
            HostMessage hostMessage = queuedHostMessages.take();
            if (hostMessage.getMessageType() != CoreMessage.TYPE.CMD_TASK_KICK) {
                log.warn("[task.runner] unknown command type : " + hostMessage.getMessageType());
                return null;
            }
            return (CommandTaskKick)hostMessage;
        } catch (Exception ex) {
            log.error("Error sending meshQueue message", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void disconnect() {
        try {
            if (batchControlConsumer != null) {
                batchControlConsumer.close();
            }
        } catch (AlreadyClosedException ace) {
            // do nothing
        } catch (Exception ex) {
            log.warn("", ex);
        }
        try {
            if (queryControlProducer != null) {
                queryControlProducer.close();
            }
        } catch (Exception ex) {
            log.warn("", ex);
        }
        try {
            if (batchControlProducer != null) {
                batchControlProducer.close();
            }
        } catch (AlreadyClosedException ace) {
            // do nothing
        } catch (Exception ex) {
            log.warn("", ex);
        }
    }
}
