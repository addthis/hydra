package com.addthis.hydra.kafka.consumer;

/**
 * Reduces initialization overhead and memory use by preventing the stack
 * trace from being created.
 *
 * Used for treating Exceptions like return values. This is bad practice, but
 * occassionally needed.
 */
public class BenignKafkaException extends RuntimeException {

    public static final BenignKafkaException INSTANCE = new BenignKafkaException();

    private static final String DEFAULT_MESSAGE = "early-exiting kafka consume or decode loop, not harmful";

    public BenignKafkaException() {
        this(DEFAULT_MESSAGE);
    }

    public BenignKafkaException(String message) {
        super(message);
    }

    @Override
    public Throwable initCause(Throwable cause) {
        return this;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
