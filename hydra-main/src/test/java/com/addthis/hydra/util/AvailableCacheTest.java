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
package com.addthis.hydra.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.addthis.hydra.job.store.AvailableCache;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AvailableCacheTest {

    private AtomicInteger fetchCount = new AtomicInteger(0);

    /**
     * Test basic behavior of AvailableCache: fetch only once per value, perform clears correctly, and handle null
     * fetched values correctly
     *
     * @throws Exception If there is a problem executing the fetch (very unlikely)
     */
    @Test
    public void testAvailableCache() throws Exception {
        AvailableCache<String> availableCache = new AvailableCache<String>(1000, 0, 50, 2) {
            @Override
            public String fetchValue(String id) {
                fetchCount.incrementAndGet();
                return "null".equals(id) ? null : id;
            }
        };
        assertEquals("should return correct value", "a", availableCache.get("a"));
        availableCache.get("a");
        availableCache.get("a");
        assertEquals("should fetch only once", 1, fetchCount.get());
        assertEquals("should return correct alternate value", "b", availableCache.get("b"));
        availableCache.get("a");
        availableCache.get("b");
        assertNull("should handle null return correctly", availableCache.get("null"));
        assertEquals("should do three fetches total", 3, fetchCount.get());
        availableCache.clear();
        assertEquals("should get correct value after clear", "a", availableCache.get("a"));
        assertEquals("should fetch again after clear", 4, fetchCount.get());
    }
}
