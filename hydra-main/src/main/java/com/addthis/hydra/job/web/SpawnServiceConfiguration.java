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
package com.addthis.hydra.job.web;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.addthis.codec.annotations.Time;
import com.addthis.codec.config.Configs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.concurrent.TimeUnit.SECONDS;

public class SpawnServiceConfiguration {

    public final int webPort;
    public final int webPortSSL;
    public final boolean requireSSL;
    public final boolean defaultSSL;
    public final int authenticationTimeout;
    public final int sudoTimeout;
    public final int groupUpdateInterval;
    public final String groupLogDir;
    public final int groupLogInterval;

    @Nullable public final String keyStorePath;
    @Nullable public final String keyStorePassword;
    @Nullable public final String keyManagerPassword;

    public static final SpawnServiceConfiguration SINGLETON;

    static {
        try {
            SINGLETON = Configs.newDefault(SpawnServiceConfiguration.class);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @JsonCreator
    public SpawnServiceConfiguration(@JsonProperty(value = "webPort", required = true) int webPort,
                                     @JsonProperty(value = "webPortSSL", required = true) int webPortSSL,
                                     @JsonProperty(value = "requireSSL", required = true) boolean requireSSL,
                                     @JsonProperty(value = "defaultSSL", required = true) boolean defaultSSL,
                                     @Time(SECONDS) @JsonProperty(value = "authTimeout", required = true) int authenticationTimeout,
                                     @Time(SECONDS) @JsonProperty(value = "sudoTimeout", required = true) int sudoTimeout,
                                     @Time(SECONDS) @JsonProperty(value = "groupUpdateInterval") int groupUpdateInterval,
                                     @Time(SECONDS) @JsonProperty(value = "groupLogInterval") int groupLogInterval,
                                     @JsonProperty(value = "groupLogDir") String groupLogDir,
                                     @JsonProperty(value = "keyStorePath") String keyStorePath,
                                     @JsonProperty(value = "keyStorePassword") String keyStorePassword,
                                     @JsonProperty(value = "keyManagerPassword") String keyManagerPassword) {
        this.webPort = webPort;
        this.webPortSSL = webPortSSL;
        this.requireSSL = requireSSL;
        this.defaultSSL = defaultSSL;
        this.authenticationTimeout = authenticationTimeout;
        this.sudoTimeout = sudoTimeout;
        this.groupUpdateInterval = groupUpdateInterval;
        this.groupLogDir = groupLogDir;
        this.groupLogInterval = groupLogInterval;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.keyManagerPassword = keyManagerPassword;
    }

}
