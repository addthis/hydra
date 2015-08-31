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

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TokenCacheTest {

    @Test
    public void expireOnWrite() throws Exception {
        TokenCache cache = new TokenCache(TokenCache.ExpirationPolicy.WRITE, 1, null);
        cache.put("foo", "bar");
        assertTrue(cache.get("foo", "bar"));
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        assertTrue(cache.get("foo", "bar"));
        Uninterruptibles.sleepUninterruptibly(600, TimeUnit.MILLISECONDS);
        assertFalse(cache.get("foo", "bar"));
    }

    @Test
    public void expireOnAccess() throws Exception {
        TokenCache cache = new TokenCache(TokenCache.ExpirationPolicy.ACCESS, 1, null);
        cache.put("foo", "bar");
        assertTrue(cache.get("foo", "bar"));
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        assertTrue(cache.get("foo", "bar"));
        Uninterruptibles.sleepUninterruptibly(600, TimeUnit.MILLISECONDS);
        assertTrue(cache.get("foo", "bar"));
        Uninterruptibles.sleepUninterruptibly(1100, TimeUnit.MILLISECONDS);
        assertFalse(cache.get("foo", "bar"));
    }

    @Test
    public void remove() throws Exception {
        TokenCache cache = new TokenCache(TokenCache.ExpirationPolicy.WRITE, 120, null);
        cache.put("foo", "bar");
        cache.put("foo", "baz");
        assertTrue(cache.get("foo", "bar"));
        assertTrue(cache.get("foo", "baz"));
        assertEquals(1, cache.remove("foo", "bar"));
        assertFalse(cache.get("foo", "bar"));
        assertTrue(cache.get("foo", "baz"));
        cache.evict("foo");
        assertFalse(cache.get("foo", "bar"));
        assertFalse(cache.get("foo", "baz"));
    }

}
