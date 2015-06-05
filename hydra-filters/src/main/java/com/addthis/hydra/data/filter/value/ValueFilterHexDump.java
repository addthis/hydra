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

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.util.regex.Pattern;

import java.nio.charset.Charset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">prints the byte representation of a string</span>.
 * <p>Example:</p>
 * <pre>
 *   {from:"USERNAME", hexdump.charset:"UTF-8"}
 * </pre>
 *
 * @user-reference
 */
public class ValueFilterHexDump extends StringFilter {

    /**
     * Character set for encoding to byte array.
     * This field is required. Default value is "UTF-8"
     */
    private final Charset charset;

    /**
     * If true then convert byte representation into a string.
     */
    private final boolean reverse;

    private static final Pattern SPACE_SEP = Pattern.compile(" ");

    @JsonCreator
    public ValueFilterHexDump(@JsonProperty(value = "charset", required = true) Charset charset,
                              @JsonProperty(value = "reverse", required = true) boolean reverse) {
        this.charset = charset;
        this.reverse = reverse;
    }

    @Override
    public String filter(String value) {
        if (reverse) {
            value = SPACE_SEP.matcher(value).replaceAll("");
            byte[] bytes = DatatypeConverter.parseHexBinary(value);
            return new String(bytes, charset);
        } else {
            StringBuilder sb = new StringBuilder();
            byte[] bytes = value.getBytes(charset);
            for (int i = 0; i < bytes.length; i++) {
                if (i > 0) {
                    sb.append(' ');
                }
                sb.append(String.format("%02x", bytes[i]));
            }
            return sb.toString();
        }
    }

}
