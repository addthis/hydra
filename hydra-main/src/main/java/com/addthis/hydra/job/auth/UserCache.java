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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.addthis.codec.annotations.Time;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UserCache {

    public enum ExpirationPolicy {
        ExpireAfterWrite, ExpireAfterAccess
    }

    /**
     * Expiration policy. Default is {@code ExpireAfterWrite}
     */
    @Nonnull
    public final ExpirationPolicy expirationPolicy;

    /**
     * Expiration time in minutes. Default is 1440.
     */
    public final int expirationTimeout;

    private final Cache<String, User> userCache;

    @JsonCreator
    public UserCache(@JsonProperty("expirationPolicy") ExpirationPolicy expirationPolicy,
                     @JsonProperty("expirationTimeout") @Time(TimeUnit.MINUTES) int expirationTimeout) {
        this.expirationPolicy = expirationPolicy;
        this.expirationTimeout = expirationTimeout;
        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        switch (expirationPolicy) {
            case ExpireAfterAccess:
                cacheBuilder = cacheBuilder.expireAfterAccess(expirationTimeout, TimeUnit.MINUTES);
                break;
            case ExpireAfterWrite:
                cacheBuilder = cacheBuilder.expireAfterWrite(expirationTimeout, TimeUnit.MINUTES);
                break;
            default:
                throw new IllegalStateException("Unknown expiration policy " + expirationPolicy);
        }
        userCache = cacheBuilder.build();
    }

    public User get(@Nonnull String name, @Nonnull UUID secret) {
        User candidate = userCache.getIfPresent(name);
        if ((candidate != null) && (candidate.secret().equals(secret))) {
            return candidate;
        } else {
            return null;
        }
    }

    public void put(@Nonnull User user) {
        userCache.put(user.name(), user);
    }

    public void remove(@Nonnull String name) {
        userCache.invalidate(name);
    }

}
