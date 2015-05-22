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

import java.util.concurrent.TimeUnit;

import com.addthis.codec.annotations.Time;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TokenCache {

    public enum ExpirationPolicy {
        WRITE, ACCESS
    }

    /**
     * Expiration policy. Default is {@code WRITE}
     */
    @Nonnull
    public final ExpirationPolicy policy;

    /**
     * Expiration time in seconds.
     */
    public final int timeout;

    private final Cache<String, String> cache;

    @JsonCreator
    public TokenCache(@JsonProperty(value = "policy", required = true) ExpirationPolicy policy,
                      @JsonProperty(value = "timeout", required = true) @Time(TimeUnit.SECONDS) int timeout) {
        this.policy = policy;
        this.timeout = timeout;
        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        switch (policy) {
            case ACCESS:
                cacheBuilder = cacheBuilder.expireAfterAccess(timeout, TimeUnit.SECONDS);
                break;
            case WRITE:
                cacheBuilder = cacheBuilder.expireAfterWrite(timeout, TimeUnit.SECONDS);
                break;
            default:
                throw new IllegalStateException("Unknown expiration policy " + policy);
        }
        cache = cacheBuilder.build();
    }

    public boolean get(@Nullable String name, @Nullable String secret) {
        if ((name == null) || (secret == null)) {
            return false;
        }
        String candidate = cache.getIfPresent(name);
        return ((candidate != null) && (candidate.equals(secret)));
    }

    public void put(@Nonnull String name, @Nonnull String secret) {
        cache.put(name, secret);
    }

    public void remove(@Nonnull String name) {
        cache.invalidate(name);
    }

}
