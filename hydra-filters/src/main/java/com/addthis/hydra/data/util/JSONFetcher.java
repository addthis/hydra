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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.LessStrings;

import com.addthis.hydra.data.filter.value.ValueFilterHttpGet;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JSONFetcher {

    private static final Logger log = LoggerFactory.getLogger(JSONFetcher.class);

    public static HashMap<String, String> staticLoadMap(String mapURL) {
        return new JSONFetcher().loadMap(mapURL);
    }

    public static HashMap<String, String> staticLoadMap(String mapURL, HashMap<String, String> map) {
        return new JSONFetcher().loadMap(mapURL, map);
    }

    public static JSONArray staticLoadJSONArray(String mapURL, int urlTimeout, int urlRetries) {
        return new JSONFetcher(urlTimeout, urlRetries, false).loadJSONArray(mapURL);
    }

    public static HashMap<String, String> staticLoadMap(String mapURL, int urlTimeout, int urlRetries, HashMap<String, String> map) {
        return new JSONFetcher(urlTimeout, urlRetries, false).loadMap(mapURL, map);
    }

    public static HashSet<String> staticLoadSet(String mapURL) {
        return new JSONFetcher().loadSet(mapURL);
    }

    public static HashSet<String> staticLoadSet(String mapURL, HashSet<String> set) {
        return new JSONFetcher().loadSet(mapURL, set);
    }

    public static HashSet<String> staticLoadSet(String mapURL, int urlTimeout, int urlRetries, HashSet<String> set) {
        return new JSONFetcher(urlTimeout, urlRetries, false).loadSet(mapURL, set);
    }

    public static HashSet<String> staticLoadCSVSet(String mapURL, int urlTimeout, int urlRetries, HashSet<String> set) {
        return new JSONFetcher(urlTimeout, urlRetries, false).loadCSVSet(mapURL, set);
    }

    private final int timeout;
    private final int retries;
    private final boolean trace;

    public JSONFetcher() {
        this(60000, false);
    }

    public JSONFetcher(int timeout, boolean trace) {
        this(timeout, 0, trace);
    }

    public JSONFetcher(int timeout, int retries, boolean trace) {
        this.timeout = timeout;
        this.retries = retries;
        this.trace = trace;
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

    public HashSet<String> loadSet(String mapURL) {
        return loadSet(mapURL, new HashSet<>());
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

    private byte[] retrieveBytes(String mapURL) {
        int retry = retries;
        while (true) {
            try {
                byte[] raw = ValueFilterHttpGet.httpGet(mapURL, null, null, timeout, trace);
                if (raw == null) {
                    throw new IllegalArgumentException("No data found at url " + mapURL);
                }
                return raw;
            } catch (Exception e) {
                log.warn("fetch err on {} for retry {} max retries is {} err is: ",
                         mapURL, (retries - retry), retries, e);
                if (retry-- > 0) {
                    continue;
                }
                throw Throwables.propagate(e);
            }
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
}
