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

        Pattern adMatcher = Pattern.compile(Parameter.value("adPattern", "(ad[0-9]+)"));
        Pattern rackMatcher = Pattern.compile(Parameter.value("rackPattern", "(rack[0-9]+)"));
        Pattern physicalHostMatcher = Pattern.compile(Parameter.value("physicalHostPattern", "(physicalHost[0-9]+)"));
        String ad = match(hostname, adMatcher);
        String rack = match(hostname, rackMatcher);
        String physicalHost = match(hostname, physicalHostMatcher);
        return new HostLocation(ad, rack, physicalHost);
    }

    private String match(String s, Pattern p) {
        Matcher matcher = p.matcher(s);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "Unknown";
    }

}
