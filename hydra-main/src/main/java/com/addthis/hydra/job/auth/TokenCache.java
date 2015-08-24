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
package com.addthis.hydra.job.auth;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;

import com.addthis.codec.annotations.Time;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenCache implements Closeable {

    public enum ExpirationPolicy {
        WRITE, ACCESS
    }

    private static final Logger log = LoggerFactory.getLogger(PermissionsManager.class);

    /**
     * Expiration policy. Default is {@code WRITE}
     */
    @Nonnull
    public final ExpirationPolicy policy;

    /**
     * Expiration time in seconds.
     */
    public final int timeout;

    /**
     * Map each username to a map of tokens to expiration timestamps.
     */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> cache;

    @Nullable
    private final Path outputPath;

    @JsonCreator
    public TokenCache(@JsonProperty(value = "policy", required = true) ExpirationPolicy policy,
                      @JsonProperty(value = "timeout", required = true) @Time(TimeUnit.SECONDS) int timeout,
                      @JsonProperty("outputPath") Path outputPath) throws IOException {
        this.policy = policy;
        this.timeout = timeout;
        this.outputPath = outputPath;
        if ((outputPath != null) && (Files.isReadable(outputPath))) {
            log.info("Loading authentication tokens from disk.");
            ObjectMapper mapper = new ObjectMapper();
            TypeFactory factory = mapper.getTypeFactory();
            this.cache = mapper.readValue(outputPath.toFile(),
                                          factory.constructMapType(ConcurrentHashMap.class,
                                                                   factory.constructType(String.class),
                                                                   factory.constructMapType(ConcurrentHashMap.class,
                                                                                            String.class, Long.class)));
        } else {
            this.cache = new ConcurrentHashMap<>();
        }
    }

    private ConcurrentHashMap<String, Long>  buildCache() {
        return new ConcurrentHashMap<>();
    }

    public boolean get(@Nullable String name, @Nullable String secret) {
        long now = System.currentTimeMillis();
        if ((name == null) || (secret == null)) {
            return false;
        }
        ConcurrentHashMap<String, Long> userTokens = cache.computeIfAbsent(name, (k) -> buildCache());
        Long expiration = userTokens.compute(secret, (key, prev) -> {
            if (prev == null) {
                return null;
            } else if (policy == ExpirationPolicy.ACCESS) {
                return (now + TimeUnit.SECONDS.toMillis(timeout));
            } else {
                return prev;
            }
        });
        if (expiration == null) {
            return false;
        } else if (expiration < now) {
            userTokens.remove(secret);
            return false;
        } else {
            return true;
        }
    }

    public void put(@Nonnull String name, @Nonnull String secret) {
        ConcurrentHashMap<String, Long> userTokens = cache.computeIfAbsent(name, (k) -> buildCache());
        userTokens.put(secret, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeout));
    }

    public int remove(@Nonnull String name, @Nonnull String secret) {
        ConcurrentHashMap<String, Long> userTokens = cache.computeIfAbsent(name, (k) -> buildCache());
        userTokens.remove(secret);
        return userTokens.size();
    }

    public void evict(@Nonnull String name) {
        cache.remove(name);
    }

    @Override
    public void close() throws IOException {
        if (outputPath != null) {
            log.info("Persisting authentication tokens to disk.");
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(outputPath.toFile(), cache);
            Files.setPosixFilePermissions(outputPath,
                                          ImmutableSet.of(PosixFilePermission.OWNER_READ,
                                                          PosixFilePermission.OWNER_WRITE));
        }
    }
}
