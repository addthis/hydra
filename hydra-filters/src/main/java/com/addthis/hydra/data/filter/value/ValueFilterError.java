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
package com.addthis.hydra.data.filter.value;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;

import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.EXTERNAL_PROPERTY;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.MINIMAL_CLASS;

/**
 * This {@link ValueFilter ValueFilter} <span class="hydra-summary">throws an exception</span>.
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
public class ValueFilterError extends ValueFilter {

    static final Logger log = LoggerFactory.getLogger(ValueFilterError.class);

    @JsonTypeInfo(use = MINIMAL_CLASS, include = EXTERNAL_PROPERTY, property = "type",
            defaultImpl = RuntimeException.class)
    @JsonProperty private Throwable message;

    @Nullable @Override public ValueObject filterValue(@Nullable ValueObject value) {
        message.fillInStackTrace();
        throw Throwables.propagate(message);
    }

    @Override public void open() {

    }
}
