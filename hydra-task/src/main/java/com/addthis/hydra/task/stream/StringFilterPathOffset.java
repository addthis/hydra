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

package com.addthis.hydra.task.stream;

import com.addthis.codec.Codec;
import com.addthis.hydra.data.filter.value.StringFilter;

/**
 * Similar to a string slice and a substring in one, but since
 * it only does one actual string creation may possibly be more
 * efficient. Probably not worth keeping around but left in for
 * now to reduce variables during other improvements.
 */
public class StringFilterPathOffset extends StringFilter {

    /**
     * When selecting a substring of the input files for either sorting the file names
     * or fetching the file paths then use this token as the path separator. Required.
     */
    @Codec.Set(codable = true, required = true)
    private String separatorToken;

    /**
     * shift the string path by this many characters. Default is 0.
     */
    @Codec.Set(codable = true)
    private int pathOffset;

    /**
     * skip this number of seperator tokens for generating file paths.
     */
    @Codec.Set(codable = true)
    private int pathTokenOffset;

    public StringFilterPathOffset(String separatorToken, int pathTokenOffset) {
        this.separatorToken = separatorToken;
        this.pathTokenOffset = pathTokenOffset;
    }

    public StringFilterPathOffset(String separatorToken, int pathTokenOffset, int pathOffset) {
        this.separatorToken = separatorToken;
        this.pathOffset = pathOffset;
        this.pathTokenOffset = pathTokenOffset;
    }

    @Override
    public String filter(String value) {
        int pathOff = pathOffset;
        if (pathTokenOffset > 0) {
            int pos = 0;
            int off = pathTokenOffset;
            while ((off > 0) && ((pos = value.indexOf(separatorToken, pos)) >= 0)) {
                off--;
                pos++;
            }
            if (pos > 0) {
                pathOff += pos;
            }
        }
        return value.substring(pathOff);
    }
}
