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

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.BundleField;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.annotations.FieldConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class BundleFilterTemplate extends BundleFilter {

    private static final Logger log = LoggerFactory.getLogger(BundleFilterTemplate.class);

    public static BundleFilterTemplate create(String tokens[], String set) {
        BundleFilterTemplate bft = new BundleFilterTemplate();
        bft.tokens = tokens;
        bft.set = set;
        return bft;
    }

    @FieldConfig(codable = true, required = true)
    private String tokens[];
    @FieldConfig(codable = true, required = true)
    private String set;

    private String fieldSet[];
    private Token  tokenSet[];

    @Override
    public void initialize() {
        ArrayList<Token> newtokens = new ArrayList<Token>();
        ArrayList<String> newfields = new ArrayList<String>();
        int pos = 0;
        for (String tok : tokens) {
            if (tok.startsWith("{{") && tok.endsWith("}}")) {
                newtokens.add(new FieldToken(pos++));
                newfields.add(tok.substring(2, tok.length() - 2));
            } else {
                newtokens.add(new StaticToken(tok));
            }
        }
        newfields.add(set);
        fieldSet = newfields.toArray(new String[newfields.size()]);
        tokenSet = newtokens.toArray(new Token[newtokens.size()]);
    }

    @Override
    public boolean filterExec(Bundle bundle) {
        try {
            BundleField bound[] = getBindings(bundle, fieldSet);
            StringBuilder sb = new StringBuilder();
            for (Token token : tokenSet) {
                sb.append(token.value(bundle, bound));
            }
            bundle.setValue(bound[bound.length - 1], ValueFactory.create(sb.toString()));
            return true;
        } catch (Exception e) {
            log.warn("", e);
            return false;
        }
    }

    public String template(Bundle bundle) {
        filter(bundle);
        BundleField bound[] = getBindings(bundle, fieldSet);
        return bundle.getValue(bound[bound.length - 1]).toString();
    }

    interface Token {

        public String value(Bundle bundle, BundleField fields[]);
    }

    class StaticToken implements Token {

        private String value;

        StaticToken(String value) {
            this.value = value;
        }

        public String value(Bundle bundle, BundleField fields[]) {
            return value;
        }
    }

    class FieldToken implements Token {

        private int pos;

        FieldToken(int pos) {
            this.pos = pos;
        }

        @Override
        public String value(Bundle bundle, BundleField fields[]) {
            return bundle.getValue(fields[pos]).toString();
        }
    }
}
