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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.addthis.bundle.channel.DataChannelError;
import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueMapEntry;
import com.addthis.bundle.value.ValueObject;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
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
    @JsonProperty(required = true) private String[] fields;

    @JsonProperty private String requestType = "POST";

    @JsonProperty private int retries = 5;

    @JsonProperty private int maxConnTotal = 200;

    @JsonProperty private int maxConnPerRoute = 20;

    @JsonProperty private int timeout = 120 * 1000; // timeout in milliseconds

    private int rotation = 0;

    private ObjectWriter objectWriter = new ObjectMapper().writer();

    private RequestConfig.Builder requestBuilder = RequestConfig.custom()
                                                                .setConnectTimeout(timeout)
                                                                .setConnectionRequestTimeout(
                                                                        timeout);

    private PoolingHttpClientConnectionManager connectionManager =
            new PoolingHttpClientConnectionManager();

    private CloseableHttpClient httpClient = HttpClientBuilder.create()
                                                              .setDefaultRequestConfig(
                                                                      requestBuilder.build())
                                                              .setConnectionManager(
                                                                      connectionManager)
                                                              .setMaxConnTotal(maxConnTotal)
                                                              .setMaxConnPerRoute(maxConnPerRoute)
                                                              .build();

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

    @Override
    protected boolean dequeueWrite(List<WriteTuple> outputTuples) throws IOException {
        if (outputTuples == null || outputTuples.size() == 0) {
            return false;
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

            int retry = 0;
            while (retry < retries) {
                rotation = (rotation + 1) % endpoints.length;
                String endpoint = endpoints[rotation];
                if (retry > 0) {
                    log.info("Attempting to send to {}. Retry {} of {}", endpoint, retry, retries - 1);
                }
                CloseableHttpResponse response = null;
                try {
                    HttpEntity entity = new StringEntity(body, ContentType.APPLICATION_JSON);
                    HttpUriRequest request = buildRequest(requestType, endpoint, entity);

                    response = httpClient.execute(request);
                    EntityUtils.consume(response.getEntity());
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode >= 200 && statusCode < 300) {
                        break;
                    } else {
                        log.error("Server error: " + response.getStatusLine());
                        log.error(response.toString());
                    }
                } catch (UnsupportedEncodingException e) {
                    log.error("Encoding error", e);
                    throw e;
                } catch (ClientProtocolException e) {
                    log.error("Client communication error: ", e);
                    throw e;
                } catch (IOException e) {
                    log.error("IO exception: ", e);
                    throw e;
                } finally {
                    retry++;
                    if (response != null) {
                        response.close();
                    }
                }
            }

            if (retry == retries) {
                throw new IOException("Max retries exceeded");
            }
        }

        return true;
    }

}
