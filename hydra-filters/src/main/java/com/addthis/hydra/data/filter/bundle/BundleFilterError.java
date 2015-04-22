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

import com.addthis.bundle.core.Bundle;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.EXTERNAL_PROPERTY;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.MINIMAL_CLASS;

/**
 * This {@link BundleFilter} <span class="hydra-summary">throws an exception</span>.
 * <p>This filter can be used either by developers when testing out the error handling
 * of the system or it can be used by users when they want to explicitly trigger the
 * error of a job.
 * </p>
 * <p>Example:</p>
 * <pre>
 *   {error.message:"Too many foos not enough bars"}
 * </pre>
 *
 * @user-reference
 */
public class BundleFilterError implements BundleFilter {

    @JsonTypeInfo(use = MINIMAL_CLASS, include = EXTERNAL_PROPERTY, property = "type",
            defaultImpl = RuntimeException.class)
    @JsonProperty private Throwable message;

    @Override public boolean filter(Bundle row) {
        message.fillInStackTrace();
        throw Throwables.propagate(message);
    }

}
