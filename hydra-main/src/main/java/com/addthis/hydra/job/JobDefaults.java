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
package com.addthis.hydra.job;

import java.io.IOException;

import java.util.Map;

import com.addthis.codec.config.Configs;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobDefaults {

    private static final Logger log = LoggerFactory.getLogger(JobDefaults.class);


    private static final JobDefaults SINGLETON;
    static {
        JobDefaults defaults;
        try {
            defaults = Configs.decodeObject(JobDefaults.class, "{}");
        } catch (IOException ex) {
            log.error("IOException retrieving job defaults: ", ex);
            defaults = null;
        }
        SINGLETON = defaults;
    }

    private final ImmutableMap<String, String> values;

    @JsonCreator
    private JobDefaults(@JsonProperty("values") Map<String, String> values) {
        this.values = ImmutableMap.copyOf(values);
    }

    public static JobDefaults getDefaults() { return SINGLETON; }

    public ImmutableMap<String, String> getValues() { return values; }
}
