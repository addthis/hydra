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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.addthis.codec.annotations.Time;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class TokenCache {

    public enum ExpirationPolicy {
        AfterWrite, AfterAccess
    }

    /**
     * Expiration policy. Default is {@code AfterWrite}
     */
    @Nonnull
    public final ExpirationPolicy expirationPolicy;

    /**
     * Expiration time in minutes.
     */
    @Time(TimeUnit.MINUTES)
    public final int expirationTimeout;

    private final Cache<String, String> cache;

    public TokenCache(ExpirationPolicy expirationPolicy,
                      int expirationTimeout) {
        this.expirationPolicy = expirationPolicy;
        this.expirationTimeout = expirationTimeout;
        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        switch (expirationPolicy) {
            case AfterAccess:
                cacheBuilder = cacheBuilder.expireAfterAccess(expirationTimeout, TimeUnit.MINUTES);
                break;
            case AfterWrite:
                cacheBuilder = cacheBuilder.expireAfterWrite(expirationTimeout, TimeUnit.MINUTES);
                break;
            default:
                throw new IllegalStateException("Unknown expiration policy " + expirationPolicy);
        }
        cache = cacheBuilder.build();
    }

    public boolean get(@Nonnull String name, @Nonnull String secret) {
        String candidate = cache.getIfPresent(name);
        return ((candidate != null) && (Objects.equals(candidate, secret)));
    }

    public void put(@Nonnull String name, @Nonnull String secret) {
        cache.put(name, secret);
    }

    public void remove(@Nonnull String name) {
        cache.invalidate(name);
    }

}
