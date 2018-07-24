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
package com.addthis.hydra.data.tree;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.Objects;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.addthis.codec.config.Configs;
import com.addthis.codec.jackson.Jackson;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Holds tree configuration settings. Expected use case is decode this object from a job config, use it during
 * processing, and to write to a file in the tree directory for later debugging or use by the query system. This means
 * there is no need for it to be deserialized from local storage during task processing, but this is not guaranteed to
 * be the case forever (it may be useful).
 * <p/>
 * All settings that are currently a mix of various system properties should probably be moved here over time.
 */
public final class TreeConfig {
    private static final Logger log = LoggerFactory.getLogger(TreeConfig.class);
    private static final Path CONFIG_FILE = Paths.get("tree.config");

    /** How much (query) cache space should be reserved for this tree relative to normal. */
    public final double cacheWeight;
    /** (dangerous!) Forces a (query) cache weight of zero regardless of actual memory usage or cache ratio. */
    public final boolean unevictable;

    public TreeConfig(@JsonProperty("cacheWeight") double cacheWeight,
                      @JsonProperty("unevictable") boolean unevictable) {
        this.cacheWeight = cacheWeight;
        this.unevictable = unevictable;
    }

    @JsonIgnore public double cacheWeight() {
        if (unevictable) {
            return 0;
        } else {
            return cacheWeight;
        }
    }

    @Nonnull public static TreeConfig readFromDataDirectory(Path dir) {
        Path path = dir.resolve(CONFIG_FILE);
        try {
            if (Files.exists(path)) {
                return Jackson.defaultMapper().readValue(path.toFile(), TreeConfig.class);
            } else {
                return Configs.newDefault(TreeConfig.class);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void writeConfigToDataDirectory(Path dir, TreeConfig config) {
        Path path = dir.resolve(CONFIG_FILE);
        try {
            Jackson.defaultMapper().writeValue(path.toFile(), config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (this.getClass() != o.getClass())) {
            return false;
        }
        TreeConfig config = (TreeConfig) o;
        return Objects.equals(this.cacheWeight, config.cacheWeight) &&
               Objects.equals(this.unevictable, config.unevictable);
    }

    @Override public int hashCode() {
        return Objects.hash(this.cacheWeight, this.unevictable);
    }

    @Override public String toString() {
        return toStringHelper(this)
                .add("cacheWeight", cacheWeight)
                .add("unevictable", unevictable)
                .toString();
    }
}
