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
package com.addthis.hydra.job;

import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.Codable;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties({"queryTraceLevel", "consecutiveFailureThreshold"})
public final class JobQueryConfig implements Codable, Cloneable {

    private static final Logger log = LoggerFactory.getLogger(JobQueryConfig.class);

    @FieldConfig private boolean canQuery;

    public JobQueryConfig() {
        this(true);
    }

    public JobQueryConfig(boolean canQuery) {
        this.canQuery = canQuery;
    }

    public JobQueryConfig(JobQueryConfig source) {
        this.canQuery = source.canQuery;
    }

    public boolean getCanQuery() {
        return canQuery;
    }

    public void setCanQuery(boolean canQuery) {
        this.canQuery = canQuery;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("can-query", getCanQuery())
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getCanQuery());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof JobQueryConfig) {
            final JobQueryConfig other = (JobQueryConfig) obj;
            return Objects.equal(getCanQuery(), other.getCanQuery());
        } else {
            return false;
        }
    }
}
