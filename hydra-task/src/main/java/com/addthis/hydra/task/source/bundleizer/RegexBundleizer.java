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
package com.addthis.hydra.task.source.bundleizer;

import javax.annotation.Nullable;

import java.lang.reflect.Method;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.CachingField;
import com.addthis.bundle.util.NoopField;
import com.addthis.bundle.value.ValueFactory;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegexBundleizer extends NewlineBundleizer {
    private static final Logger log = LoggerFactory.getLogger(RegexBundleizer.class);
    private static final Method GROUPS_METHOD = tryGetNamedGroupsMethod();
    private static final Pattern TRYING_GROUPS_HEURISTIC = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]+)>");

    private final ImmutableList<AutoField> fields;
    private final Pattern regex;

    public RegexBundleizer(@JsonProperty("fields") ImmutableList<AutoField> fields,
                           @JsonProperty("regex") Pattern regex) {
        this.regex = regex;
        Map<String, Integer> namedGroups = tryCallNamedGroups(regex);
        if (namedGroups == null) {
            if (estimateIfPatternHasNamedGroups(regex)) {
                throw new IllegalArgumentException("Looks like named groups were used, but we can't support them");
            } else if (fields.isEmpty()) {
                throw new IllegalArgumentException("No fields were specified, and we can't support named groups");
            } else {
                this.fields = fields;
            }
        } else if (fields.isEmpty() == namedGroups.isEmpty()) {
            throw new IllegalArgumentException("Must use (exactly one of) either named groups or fields");
        } else if (!namedGroups.isEmpty()) {
            int maxGroupIndex = namedGroups.values().stream().mapToInt(Integer::intValue).max().getAsInt() - 1;
            AutoField[] fieldsFromGroups = new AutoField[maxGroupIndex + 1];
            Arrays.fill(fieldsFromGroups, new NoopField());
            namedGroups.forEach((key, value) -> fieldsFromGroups[value - 1] = new CachingField(key));
            this.fields = ImmutableList.copyOf(fieldsFromGroups);
        } else {
            this.fields = fields;
        }
    }

    @Override public Bundle bundleize(Bundle next, String line) {
        Matcher lineMatcher = regex.matcher(line);
        if (lineMatcher.matches()) {
            for (int i = 0; i < fields.size(); i++) {
                fields.get(i).setValue(next, ValueFactory.create(lineMatcher.group(i + 1)));
            }
        } else {
            return null;
        }
        return next;
    }

    private static boolean estimateIfPatternHasNamedGroups(Pattern pattern) {
        return TRYING_GROUPS_HEURISTIC.matcher(pattern.pattern()).find();
    }

    @Nullable private static Method tryGetNamedGroupsMethod() {
        try {
            Method namedGroupsMethod = Pattern.class.getDeclaredMethod("namedGroups");
            namedGroupsMethod.setAccessible(true);
            return namedGroupsMethod;
        } catch (NoSuchMethodException | SecurityException ex) {
            log.warn("Failed to reflect the Pattern.namedGroups method, so we cannot use them for field names.", ex);
            return null;
        }
    }

    @SuppressWarnings("unchecked") @Nullable private static Map<String, Integer> tryCallNamedGroups(Pattern pattern) {
        if (GROUPS_METHOD != null) {
            try {
                return (Map<String, Integer>) GROUPS_METHOD.invoke(pattern);
            } catch (ReflectiveOperationException ex) {
                log.warn("Unexpected error invoking Pattern.namedGroups, so we cannot use them for field names.", ex);
            }
        }
        return null;
    }
}
