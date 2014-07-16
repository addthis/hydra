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
package com.addthis.hydra.data.filter.bundle;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import com.addthis.basis.collect.HotMap;
import com.addthis.basis.util.Strings;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.maljson.JSONArray;
import com.addthis.maljson.JSONObject;

/**
 * This {@link BundleFilter BundleFilter} <span class="hydra-summary">does something with JSON</span>.
 * <p/>
 * <p/>
 * <p>Example:</p>
 * <pre>
 * </pre>
 *
 * @user-reference
 * @hydra-name json
 */
public class BundleFilterJSON extends BundleFilter {

    public static BundleFilterJSON create(String json, String set, BundleFilterTemplate query) {
        BundleFilterJSON bfj = new BundleFilterJSON();
        bfj.json = json;
        bfj.set = set;
        bfj.query = query;
        return bfj;
    }

    @FieldConfig(codable = true)
    private int cache = 100;
    @FieldConfig(codable = true)
    private boolean trim;
    @FieldConfig(codable = true, required = true)
    private String json;
    @FieldConfig(codable = true, required = true)
    private String set;
    @FieldConfig(codable = true, required = true)
    private BundleFilterTemplate query;

    private HotMap<String, Object> objCache;
    private HotMap<String, ArrayList<QueryToken>> tokCache;
    private String[] fields;

    @Override
    public void initialize() {
        fields = new String[]{json, set};
        objCache = new HotMap<String, Object>(new ConcurrentHashMap());
        tokCache = new HotMap<String, ArrayList<QueryToken>>(new ConcurrentHashMap());
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        BundleField[] bound = getBindings(bundle, fields);
        ValueObject bundleValue = bundle.getValue(bound[0]);
        switch (bundleValue.getObjectType()) {
            case STRING:
            case INT:
            case FLOAT:
                break;
            default:
                return true;
        }
        String value = bundleValue.asString().toString();
        if (Strings.isEmpty(value)) {
            return true;
        }
        if (trim) {
            value = value.trim();
        }
        try {
            Object o = null;
            synchronized (objCache) {
                o = objCache.get(value);
            }
            if (o == null) {
                if (value.charAt(0) == '{' && value.endsWith("}")) {
                    o = new JSONObject(value);
                } else if (value.charAt(0) == '[' && value.endsWith("]")) {
                    o = new JSONArray(value);
                } else {
                    throw new Exception("Not a JSON Object : " + value);
                }
                synchronized (objCache) {
                    objCache.put(value, o);
                    while (objCache.size() > cache) {
                        objCache.removeEldest();
                    }
                }
            }
            String qkey = query.template(bundle);
            ArrayList<QueryToken> tokens = null;
            synchronized (tokCache) {
                tokens = tokCache.get(qkey);
            }
            if (tokens == null) {
                tokens = tokenize(qkey);
                synchronized (tokCache) {
                    tokCache.put(qkey, tokens);
                }
            }
            String qval = query(tokens, o, 0);
            if (qval != null) {
                bundle.setValue(bound[1], ValueFactory.create(qval));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return true;
    }

    private String query(ArrayList<QueryToken> tokens, Object o, int index) {
        if (index < tokens.size()) {
            QueryToken qt = tokens.get(index);
            if (qt.index != null) {
                if (qt.field != null) {
                    return query(tokens, ((JSONObject) o).optJSONArray(qt.field).opt(qt.index), index + 1);
                } else {
                    return query(tokens, ((JSONArray) o).opt(qt.index), index + 1);
                }
            } else {
                return query(tokens, ((JSONObject) o).opt(qt.field), index + 1);
            }
        }
        return o.toString();
    }

    private ArrayList<QueryToken> tokenize(String query) {
        ArrayList<QueryToken> tokens = new ArrayList<QueryToken>();
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
        return tokens;
    }

    private static class QueryToken {

        String field;
        Integer index;

        @Override
        public String toString() {
            return index != null ? field + "[" + index + "]" : field;
        }
    }
}
