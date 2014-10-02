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
package com.addthis.hydra.data.filter.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueObject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class FullAutoField extends CachingField {

    @Nonnull public final String[] subNames;

    public FullAutoField(@Nonnull String name, @Nonnull String... subNames) {
        super(name);
        checkNotNull(subNames);
        checkArgument(subNames.length > 0, "list of sub names must not be empty (try using a plain AutoField)");
        this.subNames = subNames;
    }

    @Override public ValueObject<?> getValue(Bundle bundle) {
        ValueObject<?> field = super.getValue(bundle);
        for (int i = 0; (field != null) && (i < subNames.length); i++) {
            field = getSubField(field, subNames[i]);
        }
        return field;
    }

    @Override public void setValue(Bundle bundle, @Nullable ValueObject<?> value) {
        ValueObject<?> field = getLastSubField(bundle);
        setSubField(field, subNames[subNames.length - 1], value);
    }

    @Override public void removeValue(Bundle bundle) {
        ValueObject<?> field = getLastSubField(bundle);
        removeSubField(field, subNames[subNames.length - 1]);
    }

    private ValueObject<?> getLastSubField(Bundle bundle) {
        ValueObject<?> field = super.getValue(bundle);
        checkNotNull(field, "missing top level field {}", super.name);
        for (int i = 0; i < (subNames.length - 1); i++) {
            field = getSubField(field, subNames[i]);
            checkNotNull(field, "missing mid level container value {}", subNames[i]);
        }
        return field;
    }

    @Nullable private static ValueObject<?> getSubField(@Nonnull ValueObject<?> field, String name) {
        if (field.getObjectType() == ValueObject.TYPE.ARRAY) {
            return field.asArray().get(Integer.parseInt(name));
        } else {
            return field.asMap().get(name);
        }
    }

    private static void removeSubField(@Nonnull ValueObject<?> field, String name) {
        if (field.getObjectType() == ValueObject.TYPE.ARRAY) {
            field.asArray().set(Integer.parseInt(name), null);
        } else {
            field.asMap().remove(name);
        }
    }

    @SuppressWarnings("unchecked")
    private static void setSubField(@Nonnull ValueObject<?> field, String name, @Nullable ValueObject<?> value) {
        if (field.getObjectType() == ValueObject.TYPE.ARRAY) {
            field.asArray().set(Integer.parseInt(name), value);
        } else {
            field.asMap().put(name, (ValueObject) value);
        }
    }
}
