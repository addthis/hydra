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

import com.addthis.basis.util.Backoff;
import com.addthis.basis.util.Parameter;
import com.addthis.hydra.job.mq.CommandTaskKick;
import com.addthis.hydra.job.mq.CoreMessage;
import com.addthis.hydra.job.mq.HostMessage;
import com.addthis.hydra.mq.MessageConsumer;
import com.addthis.hydra.mq.MessageListener;
import com.addthis.hydra.mq.MessageProducer;
import com.addthis.hydra.mq.RabbitMQUtil;
import com.addthis.hydra.mq.RabbitMessageConsumer;
import com.addthis.hydra.mq.RabbitMessageProducer;
import com.addthis.hydra.mq.RabbitQueueingConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

/**
 * Default Messaging implementation
 */
public class MinionMQImpl implements MinionMQ {
    private static Logger log = LoggerFactory.getLogger(MinionMQImpl.class);

    private static final String batchBrokerHost = Parameter.value("batch.brokerHost", "localhost");
    private static final String batchBrokerPort = Parameter.value("batch.brokerPort", "5672");
    private static final int mqReconnectDelay = Parameter.intValue("mq.reconnect.delay", 10000);
    private static final int mqReconnectTries = Parameter.intValue("mq.reconnect.tries", 10);

    private final Backoff backoff = new Backoff(1000, 5000);

    private RabbitQueueingConsumer batchJobConsumer;
    private MessageConsumer batchControlConsumer;
    private MessageProducer queryControlProducer;
    private MessageProducer batchControlProducer;
    private Channel channel;

    @Override
    public void connect(final String uuid, final MessageListener listener) throws Exception {
        log.info("Queueing via Rabbit");
        String[] routingKeys = new String[]{ uuid, HostMessage.ALL_HOSTS };
        batchControlProducer = new RabbitMessageProducer("CSBatchControl", batchBrokerHost, Integer.valueOf(batchBrokerPort));
        queryControlProducer = new RabbitMessageProducer("CSBatchQuery", batchBrokerHost, Integer.valueOf(batchBrokerPort));
        Connection connection = RabbitMQUtil.createConnection(batchBrokerHost, Integer.valueOf(batchBrokerPort));
        channel = connection.createChannel();
        channel.exchangeDeclare("CSBatchJob", "direct");
        AMQP.Queue.DeclareOk result = channel.queueDeclare(uuid + ".batchJob", true, false, false, null);
        String queueName = result.getQueue();
        channel.queueBind(queueName, "CSBatchJob", uuid);
        channel.queueBind(queueName, "CSBatchJob", HostMessage.ALL_HOSTS);
        batchJobConsumer = new RabbitQueueingConsumer(channel);
        channel.basicConsume(queueName, false, batchJobConsumer);
        batchControlConsumer = new RabbitMessageConsumer(channel, "CSBatchControl", uuid + ".batchControl", listener, routingKeys);
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
        RabbitQueueingConsumer.Delivery delivery = null;
        try {
            delivery = batchJobConsumer.nextDelivery();
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(delivery.getBody()));
            HostMessage hostMessage = (HostMessage) ois.readObject();
            if (hostMessage.getMessageType() != CoreMessage.TYPE.CMD_TASK_KICK) {
                log.warn("[task.runner] unknown command type : " + hostMessage.getMessageType());
                return null;
            }
            CommandTaskKick kick = (CommandTaskKick) hostMessage;
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            return kick;
        } catch (InterruptedException ex) {
            log.warn("Interrupted while processing task messages");
            throw ex;
        } catch (ShutdownSignalException shutdownException) {
            log.warn("Received unexpected shutdown exception from rabbitMQ", shutdownException);
            try {
                backoff.sleep();
            } catch (InterruptedException ignored) {
                // interrupted while sleeping
            }
            return null;
        } catch (Throwable ex) {
            try {
                if (delivery != null) {
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                }
            } catch (Exception e) {
                log.warn("[task.runner] unable to nack message delivery", ex);
            }
            log.warn("[task.runner] error: " + ex);
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
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (AlreadyClosedException ace) {
            // do nothing
        } catch (Exception ex) {
            log.warn("", ex);
        }
    }
}
