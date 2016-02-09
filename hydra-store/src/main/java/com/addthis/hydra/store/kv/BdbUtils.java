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
package com.addthis.hydra.store.kv;

import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.sleepycat.je.config.EnvironmentParams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.util.Size;

final class BdbUtils {

    private static final Logger log = LoggerFactory.getLogger(BdbUtils.class);

    // borrowed from codec for now
    private static final Pattern NUMBER_UNIT = Pattern.compile("(\\d+)\\s*([^\\s\\d]+)");

    static Properties filterToBdbProps(Properties properties) {
        Properties filteredProperties = new Properties();
        try (Stream<Object> stream = properties.keySet().stream()) {
            stream.filter(key -> key instanceof String)
                  .map(key -> (String) key)
                  .filter(EnvironmentParams.SUPPORTED_PARAMS::containsKey)
                  .forEach(key -> {
                      String baseValue = properties.getProperty(key);
                      if (NUMBER_UNIT.matcher(baseValue).matches()) {
                          String convertedValue = Long.toString(Size.parse(baseValue).toBytes());
                          filteredProperties.setProperty(key, convertedValue);
                      } else {
                          filteredProperties.setProperty(key, baseValue);
                      }
                  });
        }
        return filteredProperties;
    }

    private BdbUtils() {}
}
