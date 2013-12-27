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
package com.addthis.hydra.query;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class JobFailureDetector {

    private final Cache<String, AtomicInteger> jobFailures;


    public JobFailureDetector() {
        this(5);
    }

    public JobFailureDetector(int minutes) {
        jobFailures = CacheBuilder.newBuilder().expireAfterWrite(minutes, TimeUnit.MINUTES).build();
    }


    public boolean hasFailed(String job, int threshold) {
        AtomicInteger count = jobFailures.getIfPresent(job);
        if (count == null) {
            return false;
        }
        return count.get() >= threshold;
    }

    private AtomicInteger get(String job) {
        AtomicInteger count = null;
        try {
            count = jobFailures.get(job,
                    new Callable<AtomicInteger>() {
                        @Override
                        public AtomicInteger call() {
                            return new AtomicInteger(0);
                        }
                    });
        } catch (Exception ignored) {
        }
        return count;
    }

    public void indicateFailure(String job) {
        AtomicInteger count = get(job);
        System.out.println(count);
        if (count != null) {
            count.incrementAndGet();
        }
    }

    public void indicateSuccess(String job) {
        AtomicInteger count = get(job);
        if (count != null) {
            count.set(0);
        }
    }

    public Map<String, AtomicInteger> viewJobStatus() {
        return Collections.unmodifiableMap(jobFailures.asMap());
    }

}
