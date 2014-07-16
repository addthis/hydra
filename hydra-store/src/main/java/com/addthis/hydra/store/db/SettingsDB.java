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
package com.addthis.hydra.store.db;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class SettingsDB {

    public static final String TRUE = "true";
    public static final String FALSE = "false";

    private HashMap<String, String> map = new HashMap<>();

    public void setDefaults(Map<String, String> map) {
        this.map.putAll(map);
    }

    public void setDefaults(SettingsDB settings) {
        for (String key : settings.getSupportedKeys()) {
            String value = settings.getValue(key);
            if (value != null) {
                setValue(key, value);
            }
        }
    }

    public boolean hasValue(String key) {
        return map.containsKey(key);
    }

    public Collection<String> getSupportedKeys() {
        return map.keySet();
    }

    public String getValue(String key) {
        String value = map.get(key);
        if (value == null) {
            value = System.getProperty(key);
        }
        return value;
    }

    public String setValue(String key, String value) {
        return map.put(key, value);
    }

    public String setValue(String key, long value) {
        return setValue(key, Long.toString(value));
    }

    public String setValue(String key, boolean value) {
        return setValue(key, value ? TRUE : FALSE);
    }

    public String getValue(String key, String defaultValue) {
        String v = getValue(key);
        return v != null ? v : defaultValue;
    }

    public boolean isTrue(String key) {
        String value = getValue(key);
        return value != null && !value.equals("0") && !value.equalsIgnoreCase("false");
    }

    public boolean isFalse(String key) {
        String value = getValue(key);
        return !isTrue(value);
    }

    public int getIntValue(String key, int defaultValue) {
        try {
            return Integer.parseInt(getValue(key));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public long getLongValue(String key, long defaultValue) {
        try {
            return Long.parseLong(getValue(key));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    public static long parseNumber(String num) {
        num = num.toUpperCase();
        if (num.endsWith("M") || num.endsWith("MB")) {
            return Long.parseLong(num.substring(0, num.indexOf("M"))) * 1024 * 1024;
        } else if (num.endsWith("K") || num.endsWith("KB")) {
            return Long.parseLong(num.substring(0, num.indexOf("K"))) * 1024;
        } else if (num.endsWith("G") || num.endsWith("GB")) {
            return Long.parseLong(num.substring(0, num.indexOf("G"))) * 1024 * 1024 * 1024;
        } else {
            return Long.parseLong(num);
        }
    }
}
