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

import java.io.File;
import java.io.IOException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.addthis.basis.collect.HotMap;
import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.net.http.HttpResponse;
import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessFiles;
import com.addthis.basis.util.Multidict;

import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.Codec;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.codec.json.CodecJSON;
import com.addthis.hydra.common.hash.MD5HashFunction;

import com.google.common.annotations.VisibleForTesting;

import org.apache.http.client.methods.HttpGet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValueFilterHttpGet extends StringFilter implements SuperCodable {

    private static final Logger log   = LoggerFactory.getLogger(ValueFilterHttpGet.class);
    private static final Codec  codec = CodecJSON.INSTANCE;

    @FieldConfig(codable = true)
    private int cacheSize = 1000;
    @FieldConfig(codable = true)
    private long cacheAge;
    @FieldConfig(codable = true)
    private int  timeout      = 60000;
    @FieldConfig(codable = true)
    private int  retry        = 1;
    @FieldConfig(codable = true)
    private long retryTimeout = 1000;
    @FieldConfig(codable = true, required = true)
    private String  template;
    @FieldConfig(codable = true)
    private String  missValue;
    @FieldConfig(codable = true)
    private boolean trace;
    @FieldConfig(codable = true)
    private boolean emptyOk = true;
    @FieldConfig(codable = true)
    private boolean persist;
    @FieldConfig(codable = true)
    private String persistDir = ".";

    private HotMap<String, CacheObject> cache =
            new HotMap<>(new ConcurrentHashMap());
    private AtomicBoolean               init  = new AtomicBoolean(false);
    private File persistTo;

    @VisibleForTesting
    ValueFilterHttpGet() {}

    public static class CacheObject implements Codable, Comparable<CacheObject> {

        @FieldConfig(codable = true)
        private long   time;
        @FieldConfig(codable = true)
        private String key;
        @FieldConfig(codable = true)
        private String data;

        private String hash;

        @Override
        public int compareTo(CacheObject o) {
            return (int) (time - o.time);
        }
    }

    @Override
    public void postDecode() {
        if (persist) {
            persistTo = LessFiles.initDirectory(persistDir);
            LinkedList<CacheObject> list = new LinkedList<>();
            for (File file : persistTo.listFiles()) {
                if (file.isFile()) {
                    try {
                        CacheObject cached = codec.decode(CacheObject.class, LessFiles.read(file));
                        cached.hash = file.getName();
                        list.add(cached);
                        if (log.isDebugEnabled()) {
                            log.debug("restored " + cached.hash + " as " + cached.key);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            // sort so that hot map has the most recent inserted last
            CacheObject[] sort = new CacheObject[list.size()];
            list.toArray(sort);
            Arrays.sort(sort);
            for (CacheObject cached : sort) {
                if (log.isDebugEnabled()) {
                    log.debug("insert into hot " + cached.hash + " as " + cached.key);
                }
                cache.put(cached.key, cached);
            }
        }
    }

    @Override public void preEncode() {}

    private static final class ValidationOnly extends ValueFilterHttpGet {
        @Override public void postDecode() {
            // intentionally do nothing
        }

        @Override public ValueObject filter(ValueObject value) {
            throw new UnsupportedOperationException("This class is only intended for use in construction validation.");
        }
    }

    private synchronized CacheObject cacheGet(String key) {
        return cache.get(key);
    }

    private synchronized CacheObject cachePut(String key, String value) {
        CacheObject cached = new CacheObject();
        cached.time = System.currentTimeMillis();
        cached.key = key;
        cached.data = value;
        cached.hash = MD5HashFunction.hashAsString(key);
        cache.put(cached.key, cached);
        try {
            LessFiles.write(new File(persistTo, cached.hash), codec.encode(cached), false);
            if (log.isDebugEnabled()) {
                log.debug("creating " + cached.hash + " for " + cached.key);
            }
        } catch (Exception ex)  {
            log.warn("", ex);
        }
        while (cache.size() > cacheSize) {
            CacheObject old = cache.removeEldest();
            new File(persistTo, old.hash).delete();
            if (log.isDebugEnabled()) {
                log.debug("deleted " + old.hash + " containing " + old.key);
            }
        }
        return cached;
    }

    @Override
    public String filter(String sv) {
        if (sv == null) {
            return null;
        }
        CacheObject cached = cacheGet(sv);
        if (cached == null || (cacheAge > 0 && System.currentTimeMillis() - cached.time > cacheAge)) {
            if (log.isDebugEnabled() && cached != null && cacheAge > 0 && System.currentTimeMillis() - cached.time > cacheAge) {
                log.debug("aging out, replacing " + cached.hash + " or " + cached.key);
            }
            int retries = retry;
            while (retries-- > 0) {
                try {
                    String replacement = template.replace("{{}}", sv);
                    byte[] val = httpGet(replacement, null, null, timeout, trace);
                    if (val != null && (emptyOk || val.length > 0)) {
                        cached = cachePut(sv, LessBytes.toString(val));
                        break;
                    } else if (trace) {
                        log.error("{} returned {} retries left = {}", replacement, (val != null ? val.length : -1), retries);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(retryTimeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (cached == null && missValue != null) {
                cachePut(sv, missValue);
            }
        }
        return cached != null ? cached.data : null;
    }

    public static byte[] httpGet(String url, Map<String, String> requestHeaders,
            Map<String, String> responseHeaders, int timeoutms,
            boolean traceError) throws IOException {
        HttpGet get = new HttpGet(url);
        if (requestHeaders != null) {
            for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
                get.addHeader(entry.getKey(), entry.getValue());
            }
        }
        HttpResponse response = HttpUtil.execute(get, timeoutms);
        Multidict resHeaders = response.getHeaders();
        if (responseHeaders != null && resHeaders != null) {
            for (Map.Entry<String, String> entry : resHeaders.entries()) {
                responseHeaders.put(entry.getKey(), entry.getValue());
            }
        }
        if (response.getStatus() == 200) {
            return response.getBody();
        } else {
            if (traceError) {
                log.error("{} returned {}, {}", url, response.getStatus(), response.getReason());
            }
            return null;
        }
    }
}
