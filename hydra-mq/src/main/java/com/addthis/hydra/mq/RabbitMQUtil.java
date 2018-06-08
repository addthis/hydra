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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicy;
import net.jodah.lyra.config.RetryPolicies;
import net.jodah.lyra.util.Duration;

public class RabbitMQUtil {

    /**
     * Creates a connection to a RabbitMQ cluster.
     *
     * @param addresses formatted as "host1[:port],host2[:port]", etc.
     */
    public static Connection createConnection(String addresses, String username, String password) throws IOException, TimeoutException {
        Config config = new Config()
                .withRetryPolicy(RetryPolicies.retryAlways())
                .withRecoveryPolicy(new RecoveryPolicy()
                                            .withMaxDuration(Duration.minutes(60))
                                            .withBackoff(Duration.seconds(1), Duration.seconds(5)));
        ConnectionOptions options = new ConnectionOptions().withAddresses(addresses)
                                                           .withUsername(username)
                                                           .withPassword(password);
        return Connections.create(options, config);
    }
}
