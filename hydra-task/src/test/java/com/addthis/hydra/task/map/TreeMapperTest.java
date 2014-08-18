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
package com.addthis.hydra.task.map;

import com.addthis.codec.config.Configs;
import com.addthis.hydra.task.output.TaskDataOutput;
import com.addthis.maljson.JSONArray;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigOrigin;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeMapperTest {
    private static final Logger log = LoggerFactory.getLogger(TreeMapperTest.class);

    @Test public void multiLineReport() throws Exception {
        Config treeConfig = ConfigFactory.load().getConfig("test.output.tree");
        String message = null;
        int lineNumber = -1;
        try {
            TaskDataOutput treeMapper = Configs.decodeObject(TaskDataOutput.class, treeConfig);
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
        log.info("cols {} lines {} lineNum {} es.mess {}", lineColumns, lineErrors, lineNumber, message);
        Assert.assertEquals(12, lineNumber);
    }
}
