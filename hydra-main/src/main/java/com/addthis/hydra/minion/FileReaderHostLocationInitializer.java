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
package com.addthis.hydra.minion;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;

import org.codehaus.jackson.annotate.JsonCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderHostLocationInitializer implements HostLocationInitializer{
    private static final Logger log = LoggerFactory.getLogger(Minion.class);

    private File file;

    @JsonCreator
    public FileReaderHostLocationInitializer() {
        file = new File(System.getProperty("hostlocation.file","hostlocation.conf"));
        log.info("Using FileReaderHostLocationInitializer. Reading from {}", file.getName());
        if (!file.exists()) {
            log.warn("File {} does not exist", file.getName());
        }
    }

    public HostLocation getHostLocation() {
        String dataCenter = "Unknown";
        String rack = "Unknown";
        String physicalHost = "Unknown";
        try {
            Config config = ConfigFactory.parseFile(file);
            if (config.hasPath("dataCenter")) {
                dataCenter = config.getString("dataCenter");
            }
            if (config.hasPath("rack")) {
                rack = config.getString("rack");
            }
            if (config.hasPath("physicalHost")) {
                physicalHost = config.getString("physicalHost");
            }
        } catch (Exception e) {
            log.warn("error getting host location from {}: {}", file.getName(), e);
        }
        return new HostLocation(dataCenter, rack, physicalHost);
    }
}
