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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matches values to extracted tags from raw html.
 */
public class ValueFilterGrepTags extends AbstractValueFilter {
    private static final Logger log = LoggerFactory.getLogger(ValueFilterGrepTags.class);

    /** Set of values to match against. */
    @JsonProperty(required = true) private String[] values;

    /** Tag name to search for. */
    @JsonProperty(required = true) private String tagName;

    /** Tag attribute to search for. */
    @JsonProperty(required = true) private String[] tagAttrs;

    /** Log error once for every N instances. */
    @JsonProperty private int logEveryN = 100;

    private int parserErrors = 0;

    @Override
    @Nullable
    public ValueObject filterValue(ValueObject value) {
        if (value == null) {
            return null;
        }

        String html = value.asString().asNative();
        if (html == null) {
            return null;
        }

        @Nonnull Multiset<String> valueCounts = HashMultiset.create();
        try {
            Parser parser = Parser.htmlParser().setTrackErrors(0);
            @Nonnull Document doc = parser.parseInput(html, "");
            @Nonnull Elements tags = doc.select(tagName);

            for (Element tag : tags) {
                for (String tagAttr : tagAttrs) {
                    @Nonnull String attrValue = tag.attr(tagAttr).toLowerCase();
                    for (String matchValue : values) {
                        if (attrValue.contains(matchValue)) {
                            valueCounts.add(matchValue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (parserErrors++ % logEveryN == 0) {
                log.error("Failed to extract tags due to : {} Total Parser Errors : {}",
                          e.getMessage(), parserErrors);
            }
        }

        return valueCounts.isEmpty() ? null : multisetToValueMap(valueCounts);
    }

    private static ValueMap multisetToValueMap(Multiset<String> matches) {
        ValueMap valueMap = ValueFactory.createMap();
        for (Multiset.Entry<String> match : matches.entrySet()) {
            valueMap.put(match.getElement(), ValueFactory.create(match.getCount()));
        }
        return valueMap;
    }
}
