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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import java.util.HashSet;
import java.util.Set;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMessageConsumer extends DefaultConsumer implements MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMessageConsumer.class);

    private String exchange;
    private String[] routingKeys;
    private String queueName;
    private final Set<MessageListener> messageListeners = new HashSet<>();

    public RabbitMessageConsumer(Channel channel, String exchange, String queueName, MessageListener messageListener, String... routingKey) {
        super(channel);
        this.exchange = exchange;
        this.queueName = queueName;
        this.routingKeys = routingKey;
        addMessageListener(messageListener);
        try {
            open();
        } catch (IOException e) {
            log.warn("[rabit.consumer] error starting consumer" + e, e);
        }

    }

    @Override public void open() throws IOException {
        getChannel().exchangeDeclare(exchange, "direct");
        getChannel().queueDeclare(queueName, true, false, false, null);
        for (String routingKey : routingKeys) {
            getChannel().queueBind(queueName, exchange, routingKey);
        }
        getChannel().basicConsume(queueName, true, this);

    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(body));
        try {
            Serializable message = (Serializable) ois.readObject();
            if (messageListeners.size() > 0) {
                for (MessageListener messageListener : messageListeners) {
                    messageListener.onMessage(message);
                }
            }
        } catch (ClassNotFoundException e) {
            log.warn("[rabbitConsumer] error reading message", e);
        }
    }

    @Override public boolean addMessageListener(MessageListener hostMessageListener) {
        return messageListeners.add(hostMessageListener);
    }

    @Override public boolean removeMessageListener(MessageListener hostMessageListener) {
        return messageListeners.remove(hostMessageListener);
    }

    @Override public void close() throws IOException {
        Channel channel = getChannel();
        if (channel != null) {
            channel.close();
        }
    }
}

