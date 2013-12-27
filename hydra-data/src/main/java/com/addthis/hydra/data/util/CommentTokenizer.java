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
package com.addthis.hydra.data.util;

import javax.annotation.Nonnull;

import java.util.List;

public final class CommentTokenizer {

    private int oneLineComment;
    private int multiLineComment;
    private int parameter;
    private int doubleQuote;
    private int singleQuote;
    private int offset;

    @Nonnull
    private final String input;

    private final int length;

    public CommentTokenizer(@Nonnull String input) {
        this.input = input;
        length = input.length();
        offset = 0;
        oneLineComment = -1;
        multiLineComment = -1;
        parameter = -1;
        doubleQuote = -1;
        singleQuote = -1;
    }

    private static enum TokenizerMode {
        NEUTRAL, BEGIN_SINGLE_QUOTE, BEGIN_DOUBLE_QUOTE, BEGIN_PARAMETER,
        BEGIN_SINGLELINE_COMMENT, BEGIN_MULTILINE_COMMENT
    }

    /**
     * Convert -1 values returned by {@link String#indexOf(int)}
     * into the string length.
     *
     * @param offset return value of {@link String#indexOf(int)}
     * @param length string length
     * @return converted offset
     */
    private static int convertMissing(int offset, int length) {
        return (offset == -1) ? length : offset;
    }

    private static int min(int a, int b, int c, int d, int e) {
        int x = Math.min(a, b);
        int y = Math.min(c, d);
        int z = Math.min(x, y);
        return Math.min(z, e);
    }

    private int nonEscapingUpdate(int position, String search) {
        if (position < offset) {
            position = convertMissing(input.indexOf(search, offset), length);
        }

        return position;
    }

    private int escapingUpdate(int position, String search) {
        if (position < offset) {
            position = input.indexOf(search, offset);
            while (position > 0 && input.charAt(position - 1) == '\\') {
                position = input.indexOf(search, position + 1);
            }
            position = convertMissing(position, length);
        }
        return position;
    }

    private void updatePositions() {
        oneLineComment = nonEscapingUpdate(oneLineComment, "//");
        multiLineComment = nonEscapingUpdate(multiLineComment, "/*");
        parameter = nonEscapingUpdate(parameter, "%[");

        doubleQuote = escapingUpdate(doubleQuote, "\"");
        singleQuote = escapingUpdate(singleQuote, "\'");
    }

    /**
     * @return splits the supplied string and returns the parts
     */
    public void tokenize(List<String> content, List<String> delimiters) {
        assert (content != null && content.size() == 0);
        assert (delimiters != null && delimiters.size() == 0);

        TokenizerMode mode = TokenizerMode.NEUTRAL;

        if (length == 0) {
            delimiters.add("");
            content.add("");
            return;
        }

        while (offset < length) {
            switch (mode) {
                case NEUTRAL: {
                    updatePositions();

                    int smallest = min(oneLineComment, multiLineComment, parameter, doubleQuote, singleQuote);
                    if (smallest == length) {
                        delimiters.add("");
                        content.add(input.substring(offset));
                        offset = length;
                    } else if (smallest == doubleQuote) {
                        delimiters.add("\"");
                        content.add(input.substring(offset, doubleQuote));
                        offset = doubleQuote + 1;
                        mode = TokenizerMode.BEGIN_DOUBLE_QUOTE;
                    } else if (smallest == singleQuote) {
                        delimiters.add("\'");
                        content.add(input.substring(offset, singleQuote));
                        offset = singleQuote + 1;
                        mode = TokenizerMode.BEGIN_SINGLE_QUOTE;
                    } else if (smallest == oneLineComment) {
                        delimiters.add("//");
                        content.add(input.substring(offset, oneLineComment));
                        offset = oneLineComment + 2;
                        mode = TokenizerMode.BEGIN_SINGLELINE_COMMENT;
                    } else if (smallest == multiLineComment) {
                        delimiters.add("/*");
                        content.add(input.substring(offset, multiLineComment));
                        offset = multiLineComment + 2;
                        mode = TokenizerMode.BEGIN_MULTILINE_COMMENT;
                    } else if (smallest == parameter) {
                        delimiters.add("%[");
                        content.add(input.substring(offset, parameter));
                        offset = parameter + 2;
                        mode = TokenizerMode.BEGIN_PARAMETER;
                    } else {
                        throw new IllegalStateException();
                    }
                    break;
                }
                case BEGIN_SINGLE_QUOTE: {
                    int singleQuote = input.indexOf("\'", offset);
                    while (singleQuote > 0 && input.charAt(singleQuote - 1) == '\\') {
                        singleQuote = input.indexOf("\'", singleQuote + 1);
                    }
                    singleQuote = convertMissing(singleQuote, length);
                    content.add(input.substring(offset, singleQuote));
                    if (singleQuote == length) {
                        delimiters.add("");
                    } else {
                        delimiters.add("\'");
                    }
                    offset = singleQuote + 1;
                    mode = TokenizerMode.NEUTRAL;
                    break;
                }
                case BEGIN_DOUBLE_QUOTE: {
                    int doubleQuote = input.indexOf("\"", offset);
                    while (doubleQuote > 0 && input.charAt(doubleQuote - 1) == '\\') {
                        doubleQuote = input.indexOf("\"", doubleQuote + 1);
                    }
                    doubleQuote = convertMissing(doubleQuote, length);
                    content.add(input.substring(offset, doubleQuote));
                    if (doubleQuote == length) {
                        delimiters.add("");
                    } else {
                        delimiters.add("\"");
                    }
                    offset = doubleQuote + 1;
                    mode = TokenizerMode.NEUTRAL;
                    break;
                }
                case BEGIN_SINGLELINE_COMMENT: {
                    int newline = convertMissing(input.indexOf("\n", offset), length);
                    content.add(input.substring(offset, newline));
                    if (newline == length) {
                        delimiters.add("");
                    } else {
                        delimiters.add("\n");
                    }
                    offset = newline + 1;
                    mode = TokenizerMode.NEUTRAL;
                    break;
                }
                case BEGIN_MULTILINE_COMMENT: {
                    int endComment = convertMissing(input.indexOf("*/", offset), length);
                    content.add(input.substring(offset, endComment));
                    if (endComment == length) {
                        delimiters.add("");
                    } else {
                        delimiters.add("*/");
                    }
                    offset = endComment + 2;
                    mode = TokenizerMode.NEUTRAL;
                    break;
                }
                case BEGIN_PARAMETER: {
                    int endParameter = convertMissing(input.indexOf("]%", offset), length);
                    content.add(input.substring(offset, endParameter));
                    if (endParameter == length) {
                        delimiters.add("");
                    } else {
                        delimiters.add("]%");
                    }
                    offset = endParameter + 2;
                    mode = TokenizerMode.NEUTRAL;
                    break;
                }
            }
        }
    }
}
