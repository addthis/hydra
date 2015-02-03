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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMessageProducer implements MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMessageProducer.class);

    private String exchangeName;
    private String brokerAddresses;
    private String brokerUsername;
    private String brokerPassword;
    private Channel channel;

    private Connection connection;

    public RabbitMessageProducer(String exchangeName, String brokerAddresses, String brokerUsername, String brokerPassword) {
        this.exchangeName = exchangeName;
        this.brokerAddresses = brokerAddresses;
        this.brokerUsername = brokerUsername;
        this.brokerPassword = brokerPassword;
        try {
            open();
        } catch (IOException e) {
            log.warn("[rabbit.producer] error connecting producer: " + e, e);
        }

    }

    @Override public void open() throws IOException {
        connection = RabbitMQUtil.createConnection(brokerAddresses, brokerUsername, brokerPassword);
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
