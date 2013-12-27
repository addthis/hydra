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
package com.addthis.hydra.data.filter.util;

import javax.annotation.Nonnull;

import java.util.regex.Pattern;

public class PatternGenerator {

    @Nonnull
    public static String acceptStrings(@Nonnull String[] input) {
        StringBuilder builder = new StringBuilder();

        builder.append("(");

        for (int i = 0; i < input.length; i++) {
            builder.append(Pattern.quote(input[i]));
            if (i < input.length - 1) {
                builder.append("|");
            }
        }

        builder.append(")");

        return builder.toString();
    }

}
