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

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">performs glob expression matching on the input string</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {from:"SOURCE", to:"SOURCE", filter:{op:"glob", pattern:"Log_[0-9]*.gz"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name glob
 */
public class ValueFilterGlob extends StringFilter {

    /** Glob expression to match against. This field is required. */
    private final PathMatcher compiled;

    public ValueFilterGlob(@JsonProperty(value = "pattern", required = true) String pattern) {
        compiled = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
    }

    @Override
    @Nullable
    public String filter(String sv) {
        if (sv == null) {
            return null;
        }
        if (compiled.matches(Paths.get(sv))) {
            return sv;
        } else {
            return null;
        }
    }
}
