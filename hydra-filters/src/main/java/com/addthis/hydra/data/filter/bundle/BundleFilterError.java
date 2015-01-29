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

import java.lang.reflect.Constructor;

import com.addthis.bundle.core.Bundle;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link ValueFilter BundleFilter} <span class="hydra-summary">throws an exception</span>.
 * <p>This filter can be used either by developers when testing out the error handling
 * of the system or it can be used by users when they want to explicitly trigger the
 * error of a job.
 * </p>
 * <p>Example:</p>
 * <pre>
 *   {op:"error", message:"Too many foos not enough bars"}
 * </pre>
 *
 * @user-reference
 * @hydra-name error
 */
public class BundleFilterError extends BundleFilter {

    static final Logger log = LoggerFactory.getLogger(BundleFilterError.class);

    /**
     * A message to provide to the user. This field is required.
     */
    @FieldConfig(codable = true, required = true)
    private String message;

    /**
     * Optionally specify the fully qualified type of the exception.
     * Default is "java.lang.RuntimeException"
     */
    @FieldConfig(codable = true)
    private String type = "java.lang.RuntimeException";

    /**
     * Optionally suppress logging messages. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean suppressLogging = false;

    @Override public void open() {

    }

    @Override public boolean filter(Bundle row) {
        Exception exception = new RuntimeException(message);
        try {
            Class<?> clazz = Class.forName(type);
            Constructor<?> constructor = clazz.getConstructor(String.class);
            exception = (Exception) constructor.newInstance(message);
        } catch (Exception ex) {
            if (!suppressLogging) {
                log.warn("Error while trying to instantiate {}", type, ex);
            }
        }
        Throwables.propagate(exception);
        /**
         * Return statement is never reached.
         * Compiler doesn't know this.
         */
        return false;
    }
}
