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

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import com.addthis.basis.collect.HotMap;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.codec.codables.SuperCodable;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

/**
 * This {@link AbstractValueFilter ValueFilter} <span class="hydra-summary">parses JSON text into an object</span>.
 * <p/>
 * <p>
 * <p>Example:</p>
 * <pre>
 *       {op: "field", from: "line_item_info_map", to: "strategy", filter: {op:"json", query: "strategy"}},
 * </pre>
 *
 * @user-reference
 * @hydra-name json
 */
public class ValueFilterJSON extends AbstractValueFilter implements SuperCodable {

    @FieldConfig(codable = true)
    private int cacheSize = 1000;

    /**
     * If true, then remove whitespace from the beginning and end of the input. Default is false.
     */
    @FieldConfig(codable = true)
    private boolean trim;

    /**
     * TODO: write down the purpose of this field.
     */
    @FieldConfig(codable = true, required = true)
    private String query;

    private HotMap<String, Object> cache = new HotMap<>(new ConcurrentHashMap());
    private ArrayList<QueryToken> tokens;

    @Override
    public ValueObject filterValue(ValueObject value) {
        if ((value == null) || ((value.getObjectType() == ValueObject.TYPE.STRING) &&
                                value.asString().asNative().isEmpty())) {
            return value;
        }
        String sv = value.toString();
        if (trim) {
            sv = sv.trim();
        }
        try {
            Object o = null;
            synchronized (cache) {
                o = cache.get(sv);
            }
            if (o == null) {
                if (sv.charAt(0) == '{' && sv.endsWith("}")) {
                    o = new JSONObject(sv);
                } else if (sv.charAt(0) == '[' && sv.endsWith("]")) {
                    o = new JSONArray(sv);
                } else {
                    throw new Exception("Not a JSON Object : " + value);
                }
                synchronized (cache) {
                    cache.put(sv, o);
                    while (cache.size() > cacheSize) {
                        cache.removeEldest();
                    }
                }
            }
            return ValueFactory.create(query(o, 0));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private String query(Object o, int index) {
        if (index < tokens.size()) {
            QueryToken qt = tokens.get(index);
            if (qt.index != null) {
                if (qt.field != null) {
                    return query(((JSONObject) o).optJSONArray(qt.field).opt(qt.index), index + 1);
                } else {
                    return query(((JSONArray) o).opt(qt.index), index + 1);
                }
            } else {
                return query(((JSONObject) o).opt(qt.field), index + 1);
            }
        }
        return o.toString();
    }

    @Override
    public void postDecode() {
        tokens = new ArrayList<>();
        QueryToken current = new QueryToken();
        StringTokenizer st = new StringTokenizer(query, ".[", true);
        while (st.hasMoreTokens()) {
            String tok = st.nextToken();
            if (tok.equals(".")) {
                tokens.add(current);
                current = new QueryToken();
                continue;
            }
            if (tok.equals("[")) {
                tok = st.hasMoreTokens() ? st.nextToken() : "";
                if (tok.endsWith("]")) {
                    current.index = Integer.parseInt(tok.substring(0, tok.length() - 1));
                }
            } else {
                current.field = tok;
            }
        }
        tokens.add(current);
    }

    @Override public void preEncode() {}

    private static class QueryToken {

        String field;
        Integer index;

        @Override
        public String toString() {
            return index != null ? field + "[" + index + "]" : field;
        }
    }
}
