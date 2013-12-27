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
package com.addthis.hydra.store.util;

import java.util.Enumeration;
import java.util.Properties;

import com.sleepycat.je.EnvironmentConfig;


public class JEUtil {

    /**
     * Sets JE EnvironmentConfig params using Java System paremeters
     */
    public static String mergeSystemProperties(EnvironmentConfig eco) {
        // load properties from system je.*
        Properties prop = System.getProperties();
        StringBuilder sb = new StringBuilder();
        for (Enumeration<?> e = prop.propertyNames(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            if (!key.startsWith("je.")) {
                continue;
            }
            String val = (String) prop.getProperty(key);
            sb.append(key).append("=").append(val).append(" ");
            eco.setConfigParam(key, val);
        }
        return sb.toString();
    }
}
