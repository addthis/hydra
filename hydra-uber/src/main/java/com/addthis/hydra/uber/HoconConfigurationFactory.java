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
package com.addthis.hydra.uber;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import java.nio.charset.StandardCharsets;

import com.google.common.io.ByteStreams;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;

import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.json.JsonConfigurationFactory;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.util.Loader;

@Plugin(name = "HoconConfigurationFactory", category = "ConfigurationFactory")
@Order(8)
public class HoconConfigurationFactory extends JsonConfigurationFactory {

    private static final String[] FILE_TYPES = {".conf", ".json", ".properties"};

    private static final String DEPENDENCY = "com.typesafe.config.ConfigFactory";

    private static final int MAX_CONFIG_SIZE = 33_554_432;

    private final boolean configActive;

    public HoconConfigurationFactory() {
        if (!Loader.isClassAvailable(DEPENDENCY)) {
            LOGGER.debug("Missing dependencies for hocon support");
            configActive = false;
            return;
        }
        configActive = true;
    }

    @Override
    protected boolean isActive() {
        return configActive && super.isActive();
    }

    @Override
    public Configuration getConfiguration(final ConfigurationSource source) {
        if (!isActive()) {
            return null;
        }
        return getReformattingJsonConfiguration(source);
    }

    static Configuration getReformattingJsonConfiguration(final ConfigurationSource source) {
        try (InputStream configStream = source.getInputStream()) {
            byte[] buffer = ByteStreams.toByteArray(ByteStreams.limit(configStream, MAX_CONFIG_SIZE));
            String configString = new String(buffer, StandardCharsets.UTF_8);
            Config config = ConfigFactory.parseString(configString,
                                                      ConfigParseOptions.defaults()
                                                                        .setOriginDescription(source.getLocation()));
            // if it kinda looks like a job config, support its overrides and ignore everything else
            if (config.hasPath("global")) {
                config = config.getConfig("global")
                               .withFallback(ConfigFactory.load()).resolve()
                               .getConfig("logging");
            }
            String json = config.resolve().root().render(ConfigRenderOptions.concise());
            InputStream fakeStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
            ConfigurationSource fakeSource;
            if (source.getFile() != null) {
                fakeSource = new ConfigurationSource(fakeStream, source.getFile());
            } else if (source.getURL() != null) {
                fakeSource = new ConfigurationSource(fakeStream, source.getURL());
            } else {
                fakeSource = new ConfigurationSource(fakeStream);
            }
            return new HoconConfiguration(fakeSource);
        } catch (final Exception ex) {
            LOGGER.error("Error parsing {}", source.getLocation(), ex);
        }
        return null;
    }

    @Override
    public String[] getSupportedTypes() {
        return FILE_TYPES;
    }
}
