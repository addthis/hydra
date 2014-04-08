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

import com.addthis.basis.util.Parameter;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.job.mq.HostState;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.hydra.mq.RabbitMessageConsumer;
import com.addthis.hydra.mq.RabbitMessageProducer;
import com.addthis.hydra.mq.ZkMessageConsumer;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
public class SpawnMQImpl implements SpawnMQ {

    public static final int RABBIT_RECONNECT_INTERVAL_MS = 5000;
    private static Logger log = LoggerFactory.getLogger(SpawnMQImpl.class);

    private MessageProducer batchJobProducer;

    private static String batchBrokerHost = Parameter.value("batch.brokerHost", "localhost");
    private static String batchBrokerPort = Parameter.value("batch.brokerPort", "5672");

    private MessageProducer batchControlProducer;
    private MessageConsumer hostStatusConsumer;
    private MessageConsumer batchControlConsumer;
    private Channel channel;
    private Spawn spawn;
    private boolean connected;
    private final CuratorFramework zkClient;

    private final ExecutorService connectionExecutorService = MoreExecutors
            .getExitingExecutorService(new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(),
                    new ThreadFactoryBuilder().setNameFormat("rabbitMQConnectionService-%d").build()));

    private final AtomicInteger inHandler = new AtomicInteger(0);
    private Gauge<Integer> heartbeat = Metrics.newGauge(SpawnMQImpl.class, "heartbeat", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return connected ? 1 : 0;
        }
    });

    public SpawnMQImpl(CuratorFramework zkClient, Spawn spawn) {
        this.spawn = spawn;
        this.zkClient = zkClient;
    }

    @Override
    public void connectToMQ(String hostUUID) {
        hostStatusConsumer = new ZkMessageConsumer<>(zkClient, "/minion", this, HostState.class);
        connectionExecutorService.execute(new RabbitConnectionService(this, hostUUID));
    }

    private class RabbitConnectionService extends Thread {
        private final String hostUUID;
        private final SpawnMQImpl _this;

        private RabbitConnectionService(SpawnMQImpl _this, String hostUUID) {
            this._this = _this;
            this.hostUUID = hostUUID;
            this.setDaemon(true);
        }

        @Override
        public void run() {
            connect();
        }

        public void connect() {
            synchronized (this) {
                if (!connected) {
                    batchJobProducer = new RabbitMessageProducer("CSBatchJob", "localhost");
                    batchControlProducer = new RabbitMessageProducer("CSBatchControl", batchBrokerHost, Integer.valueOf(batchBrokerPort));
                    com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
                    factory.setHost(batchBrokerHost);
                    factory.setPort(Integer.valueOf(batchBrokerPort));
                    try {
                        com.rabbitmq.client.Connection connection = factory.newConnection();
                        connection.addShutdownListener(new ShutdownListener() {
                            @Override
                            public void shutdownCompleted(ShutdownSignalException e) {
                                connected = false;
                                log.warn("rabbit shutdown message received: " + e.getReason() + " will attempt to reconnect");
                                scheduleReconnect();
                            }
                        });
                        channel = connection.createChannel();
                        batchControlConsumer = new RabbitMessageConsumer(channel, "CSBatchControl", hostUUID + ".batchControl", _this, "SPAWN");
                        connected = true;
                    } catch (IOException e) {
                        log.error("Exception connection to broker: " + batchBrokerHost + ":" + batchBrokerPort, e);
                        scheduleReconnect();
                    }
                }
            }
        }

        private void scheduleReconnect() {
            try {
                Thread.sleep(RABBIT_RECONNECT_INTERVAL_MS);
                connect();
            } catch (InterruptedException e) {
                // do nothing
            }

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
        try {
            if (channel != null) channel.close();
        } catch (Exception ex)  {
            log.warn("", ex);
        }
    }
}
