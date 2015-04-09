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
package com.addthis.hydra.task.source.bundleizer;

import java.util.List;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.util.Tokenizer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * chops strings into columns (no keys)
 *
 */
public class ColumnBundleizer extends NewlineBundleizer {

    private final String[] columns;

    private final Tokenizer tokens;

    private final ValueFilter tokenFilter;

    @JsonCreator
    public ColumnBundleizer(@JsonProperty(value = "columns", required = true) String[] columns,
                            @JsonProperty(value = "tokens", required = true) Tokenizer tokens,
                            @JsonProperty(value = "tokenFilter") ValueFilter tokenFilter) {
        this.columns = columns;
        this.tokens = tokens;
        this.tokenFilter =tokenFilter;
    }

    @Override
    public Bundle bundleize(Bundle next, String line) {
        List<String> row = tokens.tokenize(line);
        if (row == null) {
            return null;
        }
        int pos = 0;
        for (String col : row) {
            if (pos >= columns.length) {
                break;
            }
            ValueObject val = ValueFactory.create(col);
            if (tokenFilter != null) {
                val = tokenFilter.filter(val, next);
            }
            next.setValue(next.getFormat().getField(columns[pos++]), val);
        }
        return next;
    }
}

