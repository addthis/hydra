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
package com.addthis.hydra.task.run;

import javax.annotation.Syntax;

import com.addthis.codec.jackson.CodecJackson;
import com.addthis.codec.jackson.Jackson;
import com.addthis.maljson.JSONArray;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskRunnerTest {

    private static final Logger log = LoggerFactory.getLogger(TaskRunnerTest.class);

    @Test
    public void multiLineReport() throws Exception {
        @Syntax("HOCON") String taskDef = "source.const.bundles: {a = 3},\n" +
                                          "\n" +
                                          "map.filterOut:[\n" +
                                          "    {op:debug, every:10}\n" +
                                          "    {SOME-NOT-REAL-FILTER {}}\n" +
                                          "]\n" +
                                          "\n" +
                                          "output: [\n" +
                                          "    {empty {}}\n" +
                                          "]";
        CodecJackson defaultCodec = Jackson.defaultCodec();
        Config config = ConfigFactory.parseString(taskDef,
                                                  ConfigParseOptions.defaults().setOriginDescription("job.conf"));
        Config defaultGlobalDefaults = defaultCodec.getGlobalDefaults();
        Config jobConfig = config;
        CodecJackson codec;
        if (config.root().containsKey("global")) {
            jobConfig = config.root().withoutKey("global").toConfig()
                              .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
            Config globalDefaults = config.getConfig("global")
                                          .withFallback(defaultGlobalDefaults)
                                          .resolve();
            jobConfig = jobConfig.resolveWith(globalDefaults);
            codec = defaultCodec.withConfig(globalDefaults);
        } else {
            jobConfig = jobConfig.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                                 .resolveWith(defaultGlobalDefaults);
            codec = defaultCodec;
        }
        String message = null;
        int lineNumber = -1;
        try {
            TaskRunnable task = codec.decodeObject(TaskRunnable.class, jobConfig);
        } catch (ConfigException ex) {
            ConfigOrigin exceptionOrigin = ex.origin();
            message = ex.getMessage();
            if (exceptionOrigin != null) {
                lineNumber = exceptionOrigin.lineNumber();
            }
        } catch (JsonProcessingException ex) {
            JsonLocation jsonLocation = ex.getLocation();
            if (jsonLocation != null) {
                lineNumber = jsonLocation.getLineNr();
                message = "Line: " + lineNumber + " ";
            }
            message += ex.getOriginalMessage();
            if (ex instanceof JsonMappingException) {
                String pathReference = ((JsonMappingException) ex).getPathReference();
                if (pathReference != null) {
                    message += " referenced via: " + pathReference;
                }
            }
        } catch (Exception other) {
            message = other.toString();
        }
        JSONArray lineColumns = new JSONArray();
        JSONArray lineErrors = new JSONArray();
        lineErrors.put(lineNumber);
        log.debug("cols {} lines {} lineNum {} es.mess {}", lineColumns, lineErrors, lineNumber, message);
        Assert.assertEquals(5, lineNumber);
    }

}