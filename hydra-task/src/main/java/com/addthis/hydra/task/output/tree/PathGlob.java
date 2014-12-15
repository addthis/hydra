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
package com.addthis.hydra.task.output.tree;

import com.addthis.bundle.value.Numeric;
import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueBytes;
import com.addthis.bundle.value.ValueCustom;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.bundle.value.ValueSimple;
import com.addthis.bundle.value.ValueString;
import com.addthis.bundle.value.ValueTranslationException;
import com.addthis.codec.annotations.FieldConfig;
import com.addthis.hydra.data.tree.DataTreeUtil;
import com.addthis.hydra.data.tree.TreeNodeList;


/**
 * @user-reference
 * @hydra-name glob
 */
public final class PathGlob extends PathValue {

    private static final GlobValueCustom GLOB = new GlobValueCustom();

    @Override public ValueObject getPathValue(final TreeMapState state) {
        return GLOB;
    }

    public static final class GlobValueCustom implements ValueCustom<Object> {

        private GlobValueCustom() {
            super();
        }

        @Override public TYPE getObjectType() {
            return TYPE.CUSTOM;
        }

        @Override public Object asNative() {
            return DataTreeUtil.getGlobObject();
        }

        @Override public ValueBytes asBytes() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override public ValueArray asArray() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override public ValueMap asMap() {
            throw new ValueTranslationException();
        }

        @Override public Numeric asNumeric() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override public ValueLong asLong() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override public ValueDouble asDouble() throws ValueTranslationException {
            throw new ValueTranslationException();
        }

        @Override public ValueString asString() throws ValueTranslationException {
            return ValueFactory.create("*");
        }

        @Override public ValueCustom<?> asCustom() throws ValueTranslationException {
            return this;
        }

        @Override public void setValues(ValueMap valueMap) {
            // do nothing
        }

        @Override public ValueSimple asSimple() {
            return asString();
        }
    }

}
