/*
 * Copyright 2014 AddThis.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.hydra.task.output;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.Time;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.http.HttpEntity;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpOutputWriter extends AbstractOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(HttpOutputWriter.class);

    // Field names to send; same for both bundle and json output
    private final String[] fields;

    /**
     * Default is "POST"
     */
    private final String requestType;

    /**
     * Default is 5.
     */
    private final int retries;

    /**
     * Default is 200.
     */
    @SuppressWarnings("unused")
    private final int maxConnTotal;

    /**
     * Default is 20.
     */
    @SuppressWarnings("unused")
    private final int maxConnPerRoute;

    /**
     * Timeout in milliseconds. Default is 120,000.
     */
    @SuppressWarnings("unused")
    private final int timeout;

    /**
     * Maximum exponential backoff wait in milliseconds. Default is 0.
     */
    @SuppressWarnings("unused")
    private final int backoffMax;

    private int rotation = 0;

    private final ObjectWriter objectWriter = new ObjectMapper().writer();

    private final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();

    private final CloseableHttpClient httpClient;

    private final Retryer<Integer> retryer;

    @JsonCreator
    public HttpOutputWriter(@JsonProperty(value = "fields", required = true) String[] fields,
                            @JsonProperty(value = "requestType") String requestType,
                            @JsonProperty(value = "retries") int retries,
                            @JsonProperty(value = "maxConnTotal") int maxConnTotal,
                            @JsonProperty(value = "maxConnPerRoute") int maxConnPerRoute,
                            @JsonProperty(value = "timeout") @Time(TimeUnit.MILLISECONDS) int timeout,
                            @JsonProperty(value = "backoffMax") @Time(TimeUnit.MILLISECONDS) int backoffMax) {
        this.fields = fields;
        this.requestType = requestType;
        this.retries = retries;
        this.maxConnTotal = maxConnTotal;
        this.maxConnPerRoute = maxConnPerRoute;
        this.timeout = timeout;
        this.backoffMax = backoffMax;
        RequestConfig.Builder requestBuilder =
                RequestConfig.custom().setConnectTimeout(timeout).setConnectionRequestTimeout(timeout);
        httpClient =
                HttpClientBuilder.create()
                                 .setDefaultRequestConfig(requestBuilder.build())
                                 .setConnectionManager(connectionManager)
                                 .setMaxConnTotal(maxConnTotal)
                                 .setMaxConnPerRoute(maxConnPerRoute)
                                 .build();
        RetryerBuilder<Integer> retryerBuilder = RetryerBuilder
                .<Integer>newBuilder()
                .retryIfExceptionOfType(NoHttpResponseException.class)
                .retryIfResult((val) -> (val == null) || !(val >= 200 && val < 300))
                .withStopStrategy(StopStrategies.stopAfterAttempt(retries));
        if (backoffMax > 0) {
            retryerBuilder.withWaitStrategy(WaitStrategies.exponentialWait(backoffMax, TimeUnit.MILLISECONDS));
        } else {
            retryerBuilder.withWaitStrategy(WaitStrategies.noWait());
        }
        retryer = retryerBuilder.build();
    }

    private Object unbox(ValueObject val) {
        switch (val.getObjectType()) {
            case INT:
                return val.asLong().getLong();
            case FLOAT:
                return val.asDouble().getDouble();
            case STRING:
                return val.asString().asNative();
            case ARRAY:
                List<Object> retList = new LinkedList<>();
                for (ValueObject element : val.asArray()) {
                    retList.add(unbox(element));
                }
                return retList;
            case MAP:
                Map<String, Object> retMap = new HashMap<>();
                ValueMap valAsMap = val.asMap();
                for (ValueMapEntry entry : valAsMap) {
                    retMap.put(entry.getKey(), unbox(entry.getValue()));
                }
                return retMap;
        }
        throw new IllegalArgumentException("Unsupported bundle field type: " + val.getObjectType());
    }

    @Override
    protected void doCloseOpenOutputs() {
        try {
            httpClient.close();
            connectionManager.close();
        } catch (IOException ex) {
            log.error("Error attempting to close HttpOutputWriter: ", ex);
        }
    }

    private HttpUriRequest buildRequest(String requestType, String endpoint, HttpEntity entity) {
        switch (requestType) {
            case "POST":
                HttpPost post = new HttpPost(endpoint);
                post.setEntity(entity);
                return post;
            case "GET":
                return new HttpGet(endpoint);
            case "HEAD":
                return new HttpHead(endpoint);
            case "PUT":
                return new HttpPut(endpoint);
            default:
                log.error("Unsupported HTTP method: {}", requestType);
                throw new DataChannelError("Unsupported HTTP method: " + requestType);
        }
    }

    @Nonnull
    private Integer request(String[] endpoints, String body, MutableInt retry) throws IOException {
        rotation = (rotation + 1) % endpoints.length;
        String endpoint = endpoints[rotation];
        if (retry.getValue() > 0) {
            log.info("Attempting to send to {}. Retry {}", endpoint, retry.getValue());
        }
        retry.increment();
        CloseableHttpResponse response = null;
        try {
            HttpEntity entity = new StringEntity(body, ContentType.APPLICATION_JSON);
            HttpUriRequest request = buildRequest(requestType, endpoint, entity);

            response = httpClient.execute(request);
            EntityUtils.consume(response.getEntity());
            return response.getStatusLine().getStatusCode();
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    @Override
    protected void dequeueWrite(List<WriteTuple> outputTuples) throws IOException {
        if (outputTuples == null || outputTuples.size() == 0) {
            return;
        }
        Map<String, List<Map<String, Object>>> batches = new HashMap<>();
        for (WriteTuple tuple : outputTuples) {
            Bundle bundle = tuple.bundle;
            String endpoint = tuple.fileName;
            List<Map<String, Object>> batch = batches.get(endpoint);
            if (batch == null) {
                batch = new ArrayList<>();
                batches.put(endpoint, batch);
            }
            HashMap<String, Object> obj = new HashMap<>();
            for (String field : fields) {
                obj.put(field, unbox(bundle.getValue(bundle.getFormat().getField(field))));
            }
            batch.add(obj);
        }

        for (Map.Entry<String, List<Map<String, Object>>> entry : batches.entrySet()) {
            String[] endpoints = entry.getKey().split(",");
            List<Map<String, Object>> batch = entry.getValue();
            String body;
            try {
                body = objectWriter.writeValueAsString(batch);
            } catch (JsonProcessingException e) {
                log.error("Error serializing batch: ", e);
                throw e;
            }

            try {
                MutableInt retry = new MutableInt(0);
                retryer.call(() -> request(endpoints, body, retry));
            } catch (RetryException ex) {
                throw new IOException("Max retries exceeded.");
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof IOException) {
                    throw ((IOException) cause);
                } else {
                    Throwables.propagate(cause);
                }
            }
        }
    }

}
