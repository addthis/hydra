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
package com.addthis.hydra.data.filter.bundle;

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.config.Configs;
import com.addthis.codec.json.CodecJSON;
import com.addthis.maljson.JSONArray;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigOrigin;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BundleFilterConditionTest {
    private static final Logger log = LoggerFactory.getLogger(BundleFilterConditionTest.class);

    @Ignore
    @Test
    public void simpleRun() throws IOException {
        Bundle bundle = new ListBundle();
        BundleFilterCondition filter = (BundleFilterCondition) Configs.decodeObject(
                BundleFilter.class, "if {true {}}, then {log = PASSED}");
        filter.filter(bundle);
    }

    public static class FilterHolder {
        @FieldConfig public BundleFilter filter;
    }

    @Test public void multiLineReport() throws Exception {
        String filterDef = "filter = [{from: UID}\n{from: LED}\n{OBVIOUSLYNOTAFILTER {}}]";
        String message = null;
        int lineNumber = -1;
        Bundle bundle = new ListBundle();
        try {
            FilterHolder filterHolder = Configs.decodeObject(FilterHolder.class, filterDef);
            filterHolder.filter.filter(bundle);
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
        Assert.assertEquals(3, lineNumber);
    }

    @Ignore
    @Test public void simpleRunJson() throws Exception {
        String filterDef = "{filter: {op:\"condition\",\n" +
                           "\t\t\t\tifCondition:{op:\"field\", from:\"UID\",filter:{op:\"require\",contains:[\"0000000000000000\"]}},\n" +
                           "\t\t\t\tifDo:{op:\"field\", from:\"TIME\", to:\"SHARD\"},\n" +
                           "                elseDo:{op:\"field\", from:\"UID\", to:\"SHARD\"},\n" +
                           "\t\t\t}}";
        Bundle bundle = new ListBundle();
        FilterHolder filterHolder = CodecJSON.INSTANCE.decode(FilterHolder.class, filterDef.getBytes());
        filterHolder.filter.filter(bundle);
    }
}