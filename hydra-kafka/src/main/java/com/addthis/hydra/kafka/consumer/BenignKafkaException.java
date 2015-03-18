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
