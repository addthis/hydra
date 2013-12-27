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

import com.addthis.codec.Codec;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class JobQueryConfig implements Codec.Codable, Cloneable {

    private static Logger log = LoggerFactory.getLogger(JobQueryConfig.class);

    @Codec.Set(codable = true)
    private boolean canQuery;
    @Codec.Set(codable = true)
    private int queryTraceLevel;
    @Codec.Set(codable = true)
    private int consecutiveFailureThreshold;

    public JobQueryConfig() {
        this(true, 0, 100);
    }

    public JobQueryConfig(boolean canQuery, int queryTraceLevel) {
        this(canQuery, queryTraceLevel, 100);
    }


    public JobQueryConfig(boolean canQuery, int queryTraceLevel, int consecutiveFailureThreshold) {
        this.canQuery = canQuery;
        this.queryTraceLevel = queryTraceLevel;
        this.consecutiveFailureThreshold = consecutiveFailureThreshold;
    }


    public boolean getCanQuery() {
        return canQuery;
    }

    public void setCanQuery(boolean canQuery) {
        this.canQuery = canQuery;
    }

    public int getQueryTraceLevel() {
        return queryTraceLevel;
    }

    public void setQueryTraceLevel(int queryTraceLevel) {
        this.queryTraceLevel = queryTraceLevel;
    }

    public int getConsecutiveFailureThreshold() {
        return consecutiveFailureThreshold;
    }

    public void setConsecutiveFailureThreshold(int consecutiveFailureThreshold) {
        this.consecutiveFailureThreshold = consecutiveFailureThreshold;
    }

    public JobQueryConfig clone() {
        try {
            return (JobQueryConfig) super.clone();
        } catch (CloneNotSupportedException e)  {
            log.warn("", e);
            return null;
        }
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("can-query", getCanQuery())
                .add("query-trace-level", getQueryTraceLevel())
                .add("consecutiveFailureThreshold", getConsecutiveFailureThreshold())
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getCanQuery(), getQueryTraceLevel(), getConsecutiveFailureThreshold());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof JobQueryConfig) {
            final JobQueryConfig other = (JobQueryConfig) obj;
            return Objects.equal(getCanQuery(), other.getCanQuery())
                   && Objects.equal(getQueryTraceLevel(), other.getQueryTraceLevel())
                   && Objects.equal(getConsecutiveFailureThreshold(), other.getConsecutiveFailureThreshold());
        } else {
            return false;
        }
    }
}
