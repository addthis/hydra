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
package com.addthis.hydra.data.filter.lambda;

import java.io.IOException;

import com.addthis.codec.config.Configs;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestStringWithValueFilter {

    private static class TestClass {

        @JsonProperty
        StringWithValueFilter startDate;
    }

    @Test
    public void deserialize() throws IOException {
        TestClass instance = Configs.decodeObject(TestClass.class, "{startDate: hello}");
        assertEquals("hello", instance.startDate.get());
        instance = Configs.decodeObject(TestClass.class, "{startDate: {input: hello, filter.case.upper:true}}");
        assertEquals("HELLO", instance.startDate.get());
    }
}
