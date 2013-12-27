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

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.addthis.codec.Codec;

/**
 * Value filter based on the JSR 223 scripting interface, and the default
 * implementation based on Rhino, shipped with the 1.6 JDK.
 */
public class ValueFilterJavascript extends StringFilter implements Codec.SuperCodable {

    @Codec.Set(codable = true)
    private String source;
    private Filter filter;

    @Override
    public void preEncode() {
    }

    @Override
    public void postDecode() {
        if (source == null) {
            throw new IllegalArgumentException("no source specified!");
        }

        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByExtension("js");
        if (engine == null) {
            throw new IllegalArgumentException("couldn't find javascript engine!");
        }

        source = source.trim();

        StringBuffer func = new StringBuffer();
        func.append("function filter(value) {\n");
        if (!source.startsWith("return ")) {
            func.append("return ");
        }
        func.append(source.trim());
        if (!source.endsWith(";")) {
            func.append(";");
        }
        func.append("\n}");

        try {
            engine.eval(func.toString());
        } catch (Exception e) {
            throw new RuntimeException("error evaluating script", e);
        }

        filter = ((Invocable) engine).getInterface(Filter.class);
    }

    @Override
    public String filter(String value) {
        try {
            return filter.filter(value);
        } catch (Exception e) {
            throw new RuntimeException("error invoking script", e);
        }
    }

    /*
     * Abstract interface for filter functions written in another language.
     */
    public static interface Filter {

        public String filter(String value) throws Exception;
    }

    public static String toString(Object o) {
        if (o == null) {
            return null;
        }

        String s = o.toString();

        if (s != null) {
            s = s.trim();
            if (s.length() == 0) {
                s = null;
            }
        }

        return s;
    }
}
