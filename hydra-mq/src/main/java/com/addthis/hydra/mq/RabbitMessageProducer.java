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
package com.addthis.hydra.mq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMessageProducer implements MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMessageProducer.class);

    private final String exchangeName;
    private final String brokerAddresses;
    private final String brokerUsername;
    private final String brokerPassword;
    private final BlockedListener blockedListener;

    private Channel channel;
    private Connection connection;

    private RabbitMessageProducer(String exchangeName, String brokerAddresses,
                                  String brokerUsername, String brokerPassword,
                                  BlockedListener blockedListener) {
        this.exchangeName = exchangeName;
        this.brokerAddresses = brokerAddresses;
        this.brokerUsername = brokerUsername;
        this.brokerPassword = brokerPassword;
        this.blockedListener = blockedListener;
    }

    public static RabbitMessageProducer constructAndOpen(String exchangeName, String brokerAddresses,
                                                         String brokerUsername, String brokerPassword,
                                                         BlockedListener blockedListener) throws IOException {
        RabbitMessageProducer producer = new RabbitMessageProducer(exchangeName, brokerAddresses,
                                                                   brokerUsername, brokerPassword,
                                                                   blockedListener);
        producer.open();
        return producer;
    }

    private void open() throws IOException {
        connection = RabbitMQUtil.createConnection(brokerAddresses, brokerUsername, brokerPassword);
        if (blockedListener != null) {
            connection.addBlockedListener(blockedListener);
        }
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "direct");
        log.info("[rabbit.producer] connection established.");
    }

    @Override public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override public void sendMessage(Serializable message, String routingKey) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(message);
        out.close();
        channel.basicPublish(exchangeName, routingKey, null, bos.toByteArray());
        log.debug("[rabbit.producer] Sent '{}'", message);
    }
}
