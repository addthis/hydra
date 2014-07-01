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

import java.util.HashMap;
import java.util.Map;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.annotations.FieldConfig;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

/**
 * A utility to match values to extracted tags from raw html
 */
public class ValueFilterGrepTags extends ValueFilter {

    /**
     * The set of values to match against.
     */
    @FieldConfig(codable = true)
    private String values[];

    /**
     * The tag name to search for
     */
    @FieldConfig(codable = true)
    private String tagName;

    /**
     * The tag attribute to search for
     */
    @FieldConfig(codable = true)
    private String tagAttr = "src";

    @Override
    public ValueObject filterValue(ValueObject value) {
        Map<String, Integer> matches = new HashMap<>();

        if (value != null) {
            String html = value.asString().asNative();

            if (html != null) {
                Parser parser = Parser.htmlParser().setTrackErrors(0);
                Document doc = parser.parseInput(html, "");

                if (doc != null) {
                    Elements tags = doc.select(tagName);

                    if (tags != null) {
                        for (Element tag : tags) {
                            String attrValue = tag.attr(tagAttr);

                            if (attrValue != null) {
                                match(attrValue.toLowerCase(), values, matches);
                            }
                        }
                    }
                }
            }
        }

        return matches != null && matches.size() > 0 ? toValueMap(matches) : null;
    }

    private ValueObject toValueMap(Map<String, Integer> matches) {
        ValueMap valueMap = ValueFactory.createMap();

        for (String match : matches.keySet()) {
            valueMap.put(match, ValueFactory.create(matches.get(match)));
        }

        return valueMap;
    }

    private void match(String attrValue, String[] values, Map<String, Integer> matchCounts) {
        Integer count = null;

        if (attrValue != null) {
            for (String value : values) {
                if (attrValue.contains(value)) {
                    matchCounts.put(value, (count = matchCounts.get(value)) != null ? count + 1 : 1);
                }
            }
        }
    }

    public void setValues(String[] values) {
        this.values = values;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public void setTagAttr(String tagAttr) {
        this.tagAttr = tagAttr;
    }
}
