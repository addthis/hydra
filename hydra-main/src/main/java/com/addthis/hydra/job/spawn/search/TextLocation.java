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
package com.addthis.hydra.job.spawn.search;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON representation of a region of characters on a single line of text (used to highlight search results, etc.)
 */
public class TextLocation implements Comparable<TextLocation> {
    @JsonProperty
    protected final int lineNum;
    @JsonProperty
    protected final int startChar;
    @JsonProperty
    protected final int endChar;

    public TextLocation(int lineNum, int startChar, int endChar) {
        this.lineNum = lineNum;
        this.startChar = startChar;
        this.endChar = endChar;
    }

    /**
     * Compares the start of two locations, earlier location = less than
     */
    @Override
    public int compareTo(TextLocation o) {
        if (lineNum != o.lineNum) {
            return lineNum - o.lineNum;
        }

        return startChar - o.startChar;
    }

    /**
     * Returns <code>true</code> if this TextLocation fully contains another.
     *
     * @param that the other TextLocation to compare wtih
     * @return <code>true</code> if this and that location have the same line number, this starting char is
     *         >= that and this ending char is <= that; <code>false</code> otherwise
     */
    public boolean fullyContains(TextLocation that) {
        return (this.lineNum == that.lineNum) && (this.startChar <= that.startChar) && (this.endChar >= that.endChar);
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (o instanceof TextLocation) {
            TextLocation other = (TextLocation) o;
            return this.compareTo(other) == 0;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{lineNum, startChar, endChar});
    }
}
