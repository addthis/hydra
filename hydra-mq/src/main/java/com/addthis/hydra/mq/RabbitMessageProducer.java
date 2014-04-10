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

    private static Logger log = LoggerFactory.getLogger(RabbitMessageProducer.class);
    private static final int DEFAULT_PORT = 5672;
    private String brokerHost;
    private int brokerPort;
    private Channel channel;
    private Connection connection;
    private String exchangeName;

    public RabbitMessageProducer(String exchangeName, String brokerHost) {
        this(exchangeName, brokerHost, DEFAULT_PORT);
    }


    public RabbitMessageProducer(String exchangeName, String brokerHost, int brokerPort) {
        this.exchangeName = exchangeName;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        try {
            open();
        } catch (IOException e) {
            log.warn("[rabbit.producer] error connecting producer: " + e, e);
        }

    }

    public void open() throws IOException {
        connection = RabbitMQUtil.createConnection(brokerHost, brokerPort);
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "direct");
        log.info("[rabbit.producer] connection established.");
    }

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public void sendMessage(Serializable message, String routingKey) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(message);
        out.close();
        channel.basicPublish(exchangeName, routingKey, null, bos.toByteArray());
        log.debug("[rabbit.producer] Sent '{}'", message);
    }
}
