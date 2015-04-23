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

import com.addthis.basis.util.LessStrings;


/**
 * This {@link AbstractValueFilter ValueFilter}
 * <span class="hydra-summary">eliminates whitespace from the beginning and end of the input string</span>.
 * <p/>
 * <p>Example:</p>
 * <pre>
 *     {from:"TEXT", {trim {}}}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterTrim extends StringFilter {

    @Override
    public String filter(String value) {
        return LessStrings.isEmpty(value) ? value : value.trim();
    }
}
