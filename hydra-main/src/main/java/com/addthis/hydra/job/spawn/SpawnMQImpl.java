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
package com.addthis.hydra.job.spawn;

import java.io.IOException;
import java.io.Serializable;

import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.hydra.mq.RabbitMQUtil;
import com.addthis.hydra.mq.RabbitMessageConsumer;
import com.addthis.hydra.mq.RabbitMessageProducer;
import com.addthis.hydra.mq.ZkMessageConsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.apache.curator.framework.CuratorFramework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SpawnMQImpl implements SpawnMQ {

    private static final Logger log = LoggerFactory.getLogger(SpawnMQImpl.class);

    private MessageProducer batchJobProducer;

    private static final String batchBrokerHost = Parameter.value("batch.brokerHost", "localhost");
    private static final String batchBrokerPort = Parameter.value("batch.brokerPort", "5672");

    private MessageProducer batchControlProducer;
    private MessageConsumer hostStatusConsumer;
    private MessageConsumer batchControlConsumer;
    private Channel channel;
    private Spawn spawn;
    private final CuratorFramework zkClient;

    private final AtomicInteger inHandler = new AtomicInteger(0);

    public SpawnMQImpl(CuratorFramework zkClient, Spawn spawn) {
        this.spawn = spawn;
        this.zkClient = zkClient;
    }

    @Override
    public void connectToMQ(String hostUUID) {
        hostStatusConsumer = new ZkMessageConsumer<>(zkClient, "/minion", this, HostState.class);
        batchJobProducer = new RabbitMessageProducer("CSBatchJob", "localhost");
        batchControlProducer = new RabbitMessageProducer("CSBatchControl", batchBrokerHost, Integer.valueOf(batchBrokerPort));
        try {
            Connection connection = RabbitMQUtil.createConnection(batchBrokerHost, Integer.valueOf(batchBrokerPort));
            channel = connection.createChannel();
            batchControlConsumer = new RabbitMessageConsumer(channel, "CSBatchControl", hostUUID + ".batchControl", this, "SPAWN");
        } catch (IOException e) {
            log.error("Exception connection to broker: " + batchBrokerHost + ":" + batchBrokerPort, e);
        }
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
        try {
            if (channel != null) channel.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }
}
