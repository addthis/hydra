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
package com.addthis.hydra.data.filter.value;

import java.io.IOException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.net.http.HttpResponse;

import com.addthis.codec.annotations.Time;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">fetches at most one url</span>.
 *
 * <p>The value filter accepts an url as input and returns the contents of the url as output.
 * This value filter assumes all invocations will provide the same url as input. If a different input
 * is provided then an exception is thrown.</p>
 *
 * @user-reference
 */
public class ValueFilterFetchOnce extends StringFilter {

    /**
     * Number of milliseconds to wait for response.
     * Default is 30 seconds.
     */
    private final int timeout;

    @JsonCreator
    public ValueFilterFetchOnce(@JsonProperty(value = "timeout", required = true)
                                @Time(TimeUnit.MILLISECONDS) int timeout) {
        this.timeout = timeout;
    }

    private static final AtomicReferenceFieldUpdater<ValueFilterFetchOnce, String> CACHE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ValueFilterFetchOnce.class, String.class, "cache");

    private volatile String cache;

    private CompletableFuture<String> result = new CompletableFuture<>();

    @Override public String filter(String input) {
        Preconditions.checkNotNull(input, "input to fetch-once filter must be non-null");
        while (true) {
            String cacheRead = cache;
            if (cacheRead == null) {
                if (CACHE_UPDATER.compareAndSet(this, null, input)) {
                    makeHttpRequest(input);
                }
            } else if (!cacheRead.equals(input)) {
                throw new IllegalStateException("fetch attempted on multiple urls " +
                                                cacheRead + " and " + input);
            } else {
                try {
                    return result.get(timeout, TimeUnit.MILLISECONDS);
                } catch (ExecutionException ex) {
                    throw Throwables.propagate(ex.getCause());
                } catch (Exception ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }
    }

    private void makeHttpRequest(String input) {
        try {
            HttpResponse response = HttpUtil.httpGet(input, timeout);
            int status = response.getStatus();
            byte[] body = response.getBody();
            if ((status == 200) && (body != null)) {
                result.complete(new String(body));
            } if ((status == 200) && (body == null)) {
                result.completeExceptionally(new IOException("Received empty response"));
            } else {
                String message = "Received non-200 status code from request: " + status;
                if (body != null) {
                    message += " with response body " + new String(body);
                } else {
                    message += " with no response body";
                }
                result.completeExceptionally(new IOException(message));
            }
        } catch (Exception ex) {
            result.completeExceptionally(ex);
        }
    }

}
