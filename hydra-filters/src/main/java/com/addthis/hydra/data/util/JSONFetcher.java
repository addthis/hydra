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
package com.addthis.hydra.data.util;

import javax.annotation.Nonnull;

import java.io.IOException;

import java.net.URISyntaxException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.addthis.basis.net.HttpUtil;
import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessStrings;

import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.clearspring.analytics.util.Preconditions;

import com.google.common.base.Throwables;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;

import org.apache.commons.lang3.mutable.MutableInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JSONFetcher {

    private static final int DEFAULT_TIMEOUT = 60_000;

    private static final Logger log = LoggerFactory.getLogger(JSONFetcher.class);

    public static JSONArray staticLoadJSONArray(String mapURL, int urlTimeout, int urlRetries) {
        return new JSONFetcher(urlTimeout, urlRetries).loadJSONArray(mapURL);
    }

    private final int timeout;

    @Nonnull
    private final Retryer<byte[]> retryer;

    public JSONFetcher() {
        this(DEFAULT_TIMEOUT);
    }

    public JSONFetcher(int timeout) {
        this(timeout, 0);
    }

    public JSONFetcher(int timeout, int retries) {
        this(timeout, retries, 0);
    }

    public JSONFetcher(int timeout, int retries, int backoffMillis) {
        Preconditions.checkArgument(retries >= 0, "retries argument must be a non-negative integer");
        this.timeout = timeout;
        RetryerBuilder<byte[]> retryerBuilder = RetryerBuilder
                .<byte[]>newBuilder()
                .retryIfExceptionOfType(IOException.class)
                .withStopStrategy(StopStrategies.stopAfterAttempt(retries + 1));
        if (backoffMillis > 0) {
            retryerBuilder.withWaitStrategy(WaitStrategies.exponentialWait(backoffMillis, TimeUnit.MILLISECONDS));
        } else {
            retryerBuilder.withWaitStrategy(WaitStrategies.noWait());
        }
        retryer = retryerBuilder.build();
    }

    public HashMap<String, String> loadMap(String mapURL) {
        return loadMap(mapURL, new HashMap<>());
    }

    /**
     * loads a json-formatted object from an url and adds enclosing
     * curly brackets if missing
     *
     * @param mapURL
     * @return string/string map
     */
    public HashMap<String, String> loadMap(String mapURL, HashMap<String, String> map) {
        try {
            byte[] raw = retrieveBytes(mapURL);
            String kv = LessBytes.toString(raw).trim();
            if (!(kv.startsWith("{") && kv.endsWith("}"))) {
                kv = LessStrings.cat("{", kv, "}");
            }
            JSONObject o = new JSONObject(kv);
            if (map == null) {
                map = new HashMap<>();
            }
            for (String key : o.keySet()) {
                map.put(key, o.optString(key));
            }
            return map;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * loads a json-formatted array from an url and adds enclosing
     * square brackets if missing
     *
     * @param mapURL
     * @return string set
     */
    public HashSet<String> loadCSVSet(String mapURL, HashSet<String> set) {
        try {
            byte[] raw = retrieveBytes(mapURL);
            String list = LessBytes.toString(raw);

            if (set == null) {
                set = new HashSet<>();
            }

            Scanner in = new Scanner(list);

            while (in.hasNext()) {
                set.add(in.nextLine().replaceAll("^\"|\"$", ""));
            }

            return set;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public JSONArray loadJSONArray(String mapURL) {
        try {
            byte[] raw = retrieveBytes(mapURL);
            String list = LessBytes.toString(raw);
            if (!(list.startsWith("[") && list.endsWith("]"))) {
                list = LessStrings.cat("[", list, "]");
            }
            JSONArray array = new JSONArray(list);
            return array;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private byte[] request(String url, MutableInt retry) throws URISyntaxException, IOException {
        if (retry.getValue() > 0) {
            log.info("Attempting to fetch {}. Retry {}", url, retry.getValue());
        }
        retry.increment();
        try {
            return HttpUtil.httpGet(url, timeout).getBody();
        } catch (URISyntaxException u) {
            log.error("URISyntaxException on url {}", url, u);
            throw u;
        }
    }

    private byte[] retrieveBytes(String url) throws IOException {
        MutableInt retry = new MutableInt(0);
        try {
            return retryer.call(() -> request(url, retry));
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (RetryException e) {
            throw new IOException("Max retries exceeded");
        }
    }

    /**
     * loads a json-formatted array from an url and adds enclosing
     * square brackets if missing
     *
     * @param mapURL
     * @return string set
     */
    public HashSet<String> loadSet(String mapURL, HashSet<String> set) {
        try {
            byte[] raw = retrieveBytes(mapURL);
            String list = LessBytes.toString(raw);
            if (!(list.startsWith("[") && list.endsWith("]"))) {
                list = LessStrings.cat("[", list, "]");
            }
            JSONArray o = new JSONArray(list);
            if (set == null) {
                set = new HashSet<>();
            }
            for (int i = 0; i < o.length(); i++) {
                set.add(o.getString(i));
            }
            return set;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static class SetLoader {
        private final String url;
        private int timeout = DEFAULT_TIMEOUT;
        private int retries;
        private int backoff;
        private boolean csv;
        private HashSet<String> target;

        public SetLoader(String url) {
            this.url = url;
        }

        public SetLoader setContention(int timeout, int retries, int backoff) {
            this.timeout = timeout;
            this.retries = retries;
            this.backoff = backoff;
            return this;
        }

        public SetLoader setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public SetLoader setRetries(int retries) {
            this.retries = retries;
            return this;
        }

        public SetLoader setBackoff(int backoff) {
            this.backoff = backoff;
            return this;
        }

        public SetLoader setCsv(boolean csv) {
            this.csv = csv;
            return this;
        }

        public SetLoader setTarget(HashSet<String> target) {
            this.target = target;
            return this;
        }

        public HashSet<String> load() {
            JSONFetcher fetcher = new JSONFetcher(timeout, retries, backoff);
            if (csv) {
                return fetcher.loadCSVSet(url, target);
            } else {
                return fetcher.loadSet(url, target);
            }
        }
    }

}
