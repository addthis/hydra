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

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.util.Objects;


/**
 * config-defined templated parameter
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE,
                isGetterVisibility = JsonAutoDetect.Visibility.NONE,
                setterVisibility = JsonAutoDetect.Visibility.NONE)
public final class JobParameter implements Codable, Cloneable {

    @FieldConfig(codable = true)
    private String name;
    @FieldConfig(codable = true)
    private String value;
    @FieldConfig(codable = true)
    private String defaultValue;

    public JobParameter() {
    }

    public JobParameter(String name, String value, String defaultValue) {
        this.name = name;
        this.value = value;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getParamString() {
        return "%[" + (defaultValue != null ? name + ":" + defaultValue : name) + "]%";
    }


    @Override
    public boolean equals(Object o) {
        if (o.getClass() != JobParameter.class || o == null) {
            return false;
        }

        JobParameter other = (JobParameter) o;

        return Objects.equals(name, other.name) &&
                Objects.equals(value, other.value) &&
                Objects.equals(defaultValue, other.defaultValue);
    }

    @Override
    public int hashCode() {
        return (this.value != null ? this.value.hashCode() : 0) ^
                (this.name != null ? 27 * this.name.hashCode() : 0) ^
                (this.defaultValue != null ? 53 * this.defaultValue.hashCode() : 0);
    }
}
