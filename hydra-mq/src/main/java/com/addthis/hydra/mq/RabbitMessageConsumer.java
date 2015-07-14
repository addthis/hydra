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

import javax.annotation.Nonnull;

import java.io.IOException;

import java.util.HashSet;
import java.util.Set;

import com.addthis.codec.jackson.Jackson;

import com.google.common.collect.ImmutableList;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMessageConsumer<T> extends DefaultConsumer implements MessageConsumer<T> {

    private static final Logger log = LoggerFactory.getLogger(RabbitMessageConsumer.class);

    @Nonnull
    private final String exchange;
    @Nonnull
    private final ImmutableList<String> routingKeys;
    @Nonnull
    private final ImmutableList<String> closeUnbindKeys;
    @Nonnull
    private final String queueName;
    private final Set<MessageListener<T>> messageListeners = new HashSet<>();
    private final Class<T> messageType;

    public RabbitMessageConsumer(@Nonnull Channel channel, @Nonnull String exchange,
                                 @Nonnull String queueName, @Nonnull MessageListener<T> messageListener,
                                 @Nonnull ImmutableList<String> routingKey,
                                 @Nonnull ImmutableList<String> closeUnbindKeys,
                                 @Nonnull Class<T> messageType) {
        super(channel);
        this.exchange = exchange;
        this.queueName = queueName;
        this.routingKeys = routingKey;
        this.closeUnbindKeys = closeUnbindKeys;
        this.messageType = messageType;
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
    public void handleDelivery(String consumerTag, Envelope envelope,
                               AMQP.BasicProperties properties, byte[] body) throws IOException {
        try {
            T message = Jackson.defaultMapper().readValue(body, messageType);
            if (messageListeners.size() > 0) {
                for (MessageListener<T> messageListener : messageListeners) {
                    messageListener.onMessage(message);
                }
            }
        } catch (IOException e) {
            log.warn("[rabbitConsumer] error reading message", e);
        }
    }

    @Override public boolean addMessageListener(MessageListener<T> hostMessageListener) {
        return messageListeners.add(hostMessageListener);
    }

    @Override public boolean removeMessageListener(MessageListener<T> hostMessageListener) {
        return messageListeners.remove(hostMessageListener);
    }

    @Override public void close() throws IOException {
        IOException firstError = null;
        Channel channel = getChannel();
        if (channel != null) {
            for(String routingKey : closeUnbindKeys) {
                try {
                    channel.queueUnbind(queueName, exchange, routingKey);
                } catch (IOException ex) {
                    firstError = (firstError == null) ? ex : firstError;
                }
            }
            try {
                channel.close();
            } catch (IOException ex) {
                firstError = (firstError == null) ? ex : firstError;
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }
}

