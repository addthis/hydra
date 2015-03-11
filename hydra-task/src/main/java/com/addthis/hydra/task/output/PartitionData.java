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
package com.addthis.hydra.task.output;

import javax.annotation.Nullable;

import com.addthis.basis.util.LessStrings;

class PartitionData {
    private static final int DEFAULT_PADDING = 3;
    private static final String PART_PREFIX = "{{PART";
    private static final String PART_POSTFIX = "}}";

    private final String replacementString;
    private final int padTo;

    PartitionData(@Nullable String replacementString, int padTo) {
        this.replacementString = replacementString;
        this.padTo = padTo;
    }

    @Nullable public String getReplacementString() {
        return replacementString;
    }

    public int getPadTo() {
        return padTo;
    }

    protected static PartitionData getPartitionData(String target) {
        String replacement = null;
        int padTo = DEFAULT_PADDING;
        int startPartitionIndex = target.indexOf(PART_PREFIX);
        if (startPartitionIndex >= 0) {
            int closePartitionIndex = target.indexOf(PART_POSTFIX);
            if (closePartitionIndex > startPartitionIndex) {
                replacement = target.substring(startPartitionIndex, closePartitionIndex + 2);
                String[] tok = LessStrings.splitArray(target.substring(startPartitionIndex + 2, closePartitionIndex),
                                                      ":");
                if (tok.length > 1) {
                    padTo = Integer.parseInt(tok[1]);
                }
            }
        }
        return new PartitionData(replacement, padTo);
    }
}
