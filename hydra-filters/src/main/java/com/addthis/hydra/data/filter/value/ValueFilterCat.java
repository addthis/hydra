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

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">perform pre- or post- string concatenation</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *   {op:"cat", pre:"Greetings " post:" Happy Birthday!"}
 * </pre>
 *
 * @user-reference
 * @hydra-name cat
 */
public class ValueFilterCat extends AbstractValueFilter {

    /**
     * If non-null, then prefix this string onto the beginning of the input.
     */
    @FieldConfig(codable = true)
    private String pre;

    /**
     * If non-null, then postfix this string onto the end of the input.
     */
    @FieldConfig(codable = true)
    private String post;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if (value != null) {
            if (pre != null) {
                value = ValueFactory.create(pre.concat(value.toString()));
            }
            if (post != null) {
                value = ValueFactory.create(value.toString().concat(post));
            }
        }
        return value;
    }

}
