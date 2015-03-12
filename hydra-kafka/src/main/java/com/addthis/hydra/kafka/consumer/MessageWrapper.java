package com.addthis.hydra.kafka.consumer;

import kafka.message.MessageAndOffset;

class MessageWrapper {

    static final MessageWrapper messageQueueEndMarker = new MessageWrapper(null, null, null, 0, null);

    public final MessageAndOffset messageAndOffset;
    public final String host;
    public final String topic;
    public final int partition;
    public final String sourceIdentifier;

    MessageWrapper(MessageAndOffset messageAndOffset, String host, String topic, int partition, String sourceIdentifier) {
        this.messageAndOffset = messageAndOffset;
        this.host = host;
        this.topic = topic;
        this.partition = partition;
        this.sourceIdentifier = sourceIdentifier;
    }
}
