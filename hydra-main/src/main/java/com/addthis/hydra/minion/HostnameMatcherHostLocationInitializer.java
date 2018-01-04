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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.basis.util.Parameter;

import org.codehaus.jackson.annotate.JsonCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostnameMatcherHostLocationInitializer implements HostLocationInitializer{
    private static final Logger log = LoggerFactory.getLogger(HostnameMatcherHostLocationInitializer.class);

    @JsonCreator
    public HostnameMatcherHostLocationInitializer() {
        log.info("using HostnameMatcherHostLocationInitializer.");
    }

    public HostLocation getHostLocation() {

        String hostname = Parameter.value("minion.localhost");

        String dataCenterPattern = Parameter.value("minion.datacenterPattern", "");
        String rackPattern = Parameter.value("minion.rackPattern", "");
        String physicalHostMatcher = Parameter.value("minion.physicalHostPattern", "");

        String ad = match(hostname, dataCenterPattern);
        String rack = match(hostname, rackPattern);
        String physicalHost = match(hostname, physicalHostMatcher);
        return new HostLocation(ad, rack, physicalHost);
    }

    private String match(String s, String p) {
        if (p.equals("")) {
            return "Unknown";
        }
        Pattern pattern = Pattern.compile(p);
        Matcher matcher = pattern.matcher(s);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "Unknown";
    }

}
