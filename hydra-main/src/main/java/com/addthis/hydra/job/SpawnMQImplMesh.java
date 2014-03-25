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

import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.mq.MeshMessageConsumer;
import com.addthis.hydra.mq.MeshMessageProducer;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.hydra.mq.ZkMessageConsumer;
import com.addthis.meshy.MeshyClient;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class SpawnMQImplMesh implements SpawnMQ {

    private static Logger log = LoggerFactory.getLogger(SpawnMQImplMesh.class);

    private MessageProducer batchJobProducer;
    private MessageProducer batchControlProducer;
    private MessageConsumer hostStatusConsumer;
    private MessageConsumer batchControlConsumer;

    private Spawn spawn;
    private boolean connected;
    private final CuratorFramework zkClient;

    private final AtomicInteger inHandler = new AtomicInteger(0);
    private Gauge<Integer> heartbeat = Metrics.newGauge(SpawnMQImplMesh.class, "heartbeat", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return connected ? 1 : 0;
        }
    });

    public SpawnMQImplMesh(CuratorFramework zkClient, Spawn spawn) {
        log.info("Queueing via Mesh");
        this.spawn = spawn;
        this.zkClient = zkClient;
    }

    @Override
    public void connectToMQ(String hostUUID) throws Exception {
        MeshyClient mesh = spawn.getMeshyClient();
        batchJobProducer = new MeshMessageProducer(mesh, "CSBatchJob");
        batchControlProducer = new MeshMessageProducer(mesh, "CSBatchControl");
        batchControlConsumer = new MeshMessageConsumer(mesh, "CSBatchControl", "SPAWN");
        batchControlConsumer.addMessageListener(this);
        hostStatusConsumer = new ZkMessageConsumer<>(zkClient, "/minion", this, HostState.class);
        this.connected = true;
    }

    /**
     * wraps mq handler and looks for concurrent use
     */
    @Override
    public void onMessage(Serializable message) {
        if (message instanceof CoreMessage) {
            CoreMessage coreMessage = (CoreMessage) message;
            try {
                int conc = inHandler.incrementAndGet();
                if (conc > 1) {
                    log.debug("[mq.handle] concurrent={}", conc);
                    synchronized (inHandler) {
                        spawn.handleMessage(coreMessage);
                    }
                } else {
                    spawn.handleMessage(coreMessage);
                }
            } catch (Exception ex)  {
                log.warn("", ex);
            } finally {
                inHandler.decrementAndGet();
            }
        } else {
            log.warn("[spawn.mq] received unknown message type:{}", message);
        }
    }

    @Override
    public void sendControlMessage(HostMessage msg) {
        sendMessage(msg, batchControlProducer);
    }

    @Override
    public void sendJobMessage(HostMessage msg) {
        sendMessage(msg, batchJobProducer);
    }

    private void sendMessage(HostMessage msg, MessageProducer producer) {
        if (!connected) {
            throw new RuntimeException("[spawn.mq] is not connected, unable to send message: " + msg);
        }
        try {
            producer.sendMessage(msg, msg.getHostUuid());
        } catch (IOException e)  {
            log.warn("", e);
        }
    }


    @Override
    public void close() {
        try {
            if (hostStatusConsumer != null) hostStatusConsumer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        try {
            if (batchControlConsumer != null) batchControlConsumer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        try {
            if (batchControlProducer != null) batchControlProducer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        try {
            if (batchJobProducer != null) batchJobProducer.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }
}
