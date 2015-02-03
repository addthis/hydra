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
package com.addthis.hydra.task.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;

import com.addthis.bundle.util.ValueUtil;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.filter.value.ValueFilter;
import com.addthis.hydra.data.util.Tokenizer;


public class StreamTokenizer extends Tokenizer {

    @FieldConfig(codable = true)
    private ValueFilter filter;

    private BufferedReader reader;
    private String lastLine;

    /** */
    public StreamTokenizer setInputStream(InputStream stream) {
        this.reader = new BufferedReader(new InputStreamReader(stream));
        return this;
    }

    @Override
    public Tokenizer initialize() {
        super.initialize();
        return this;
    }

    /**
     * updated after each call to nextLine.
     *
     * @return raw line corresponding with tokens returned in nextLine()
     */
    public String lastRawLine() {
        return lastLine;
    }

    public boolean skipLines(long lines) throws IOException {
        while (lines > 0) {
            String line = reader.readLine();
            if (line == null) {
                return false;
            }
            if (filter != null) {
                ValueObject val = ValueFactory.create(line);
                if (filter != null) {
                    val = filter.filter(val);
                }
                if (ValueUtil.isEmpty(val)) {
                    continue;
                }
            }
            lines--;
        }
        return true;
    }

    /**
     * @return the next line from the input stream, split into tokens according
     *         to the tokenizing settings, with replacements performed
     */
    public synchronized List<String> nextLine() throws IOException {
        String line = null;
        try {
            while (true) {
                line = reader.readLine();
                if (filter != null) {
                    ValueObject val = ValueFactory.create(line);
                    if (filter != null) {
                        val = filter.filter(val);
                    }
                    if (ValueUtil.isEmpty(val)) {
                        continue;
                    }
                    line = ValueUtil.asNativeString(val);
                }
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lastLine = line;
        }
        if (line == null) {
            return null;
        }
        if (line.length() == 0) {
            return new ArrayList<>(0);
        }
        return tokenize(line);
    }

    /**
     * Closes the stream, if it's open.
     */
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
