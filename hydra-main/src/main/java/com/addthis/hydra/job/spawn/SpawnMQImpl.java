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
import java.util.concurrent.locks.ReentrantLock;

import com.addthis.basis.util.Parameter;

import com.addthis.hydra.job.minion.Minion;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.hydra.mq.RabbitMQUtil;
import com.addthis.hydra.mq.RabbitMessageConsumer;
import com.addthis.hydra.mq.RabbitMessageProducer;
import com.addthis.hydra.mq.ZkMessageConsumer;

import com.google.common.collect.ImmutableList;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.apache.curator.framework.CuratorFramework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class SpawnMQImpl implements SpawnMQ {

    private static final Logger log = LoggerFactory.getLogger(SpawnMQImpl.class);

    private static final String batchBrokeAddresses = Parameter.value("batch.brokerAddresses", "localhost:5672");
    private static final String batchBrokerUsername = Parameter.value("batch.brokerUsername", "guest");
    private static final String batchBrokerPassword = Parameter.value("batch.brokerPassword", "guest");

    private MessageProducer batchJobProducer;
    private MessageProducer batchControlProducer;
    private MessageConsumer hostStatusConsumer;
    private MessageConsumer batchControlConsumer;
    private Channel channel;
    private Spawn spawn;
    private final CuratorFramework zkClient;

    private final ReentrantLock lock = new ReentrantLock();

    public SpawnMQImpl(CuratorFramework zkClient, Spawn spawn) {
        this.spawn = spawn;
        this.zkClient = zkClient;
    }

    private static class QuiesceOnRabbitMQBlockedListener implements BlockedListener {

        private final Spawn spawn;

        QuiesceOnRabbitMQBlockedListener(Spawn spawn) {
            this.spawn = spawn;
        }

        @Override public void handleBlocked(String reason) throws IOException {
            if (!spawn.getSystemManager().isQuiesced()) {
                log.error("Spawn is quiescing itself. A rabbitMQ producer was" +
                          " blocked from producing a message due to {}", reason);
                spawn.getSystemManager().quiesceCluster(true, "rabbitmq");
            }
        }

        @Override public void handleUnblocked() throws IOException {

        }
    }

    @Override
    public void connectToMQ(String hostUUID) throws IOException {
        QuiesceOnRabbitMQBlockedListener blockedListener = new QuiesceOnRabbitMQBlockedListener(spawn);
        hostStatusConsumer = new ZkMessageConsumer<>(zkClient, "/minion", this, HostState.class);
        batchJobProducer = RabbitMessageProducer.constructAndOpen("CSBatchJob", batchBrokeAddresses,
                                                                  batchBrokerUsername, batchBrokerPassword,
                                                                  blockedListener);
        batchControlProducer = RabbitMessageProducer.constructAndOpen("CSBatchControl", batchBrokeAddresses,
                                                                      batchBrokerUsername, batchBrokerPassword,
                                                                      blockedListener);
        Connection connection = RabbitMQUtil.createConnection(batchBrokeAddresses, batchBrokerUsername,
                                                                  batchBrokerPassword);
        channel = connection.createChannel();
        batchControlConsumer = new RabbitMessageConsumer(channel, "CSBatchControl",
                                                         hostUUID + Minion.batchControlQueueSuffix,
                                                         this, ImmutableList.of("SPAWN"),
                                                         ImmutableList.of());
    }

    /**
     * wraps mq handler and looks for concurrent use
     */
    @Override
    public void onMessage(Serializable message) {
        if (message instanceof CoreMessage) {
            CoreMessage coreMessage = (CoreMessage) message;
            lock.lock();
            try {
                spawn.handleMessage(coreMessage);
            } catch (Exception ex)  {
                log.warn("Error sending message {} to host {}: ", coreMessage.getMessageType(),
                        coreMessage.getHostUuid(), ex);
            } finally {
                lock.unlock();
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
