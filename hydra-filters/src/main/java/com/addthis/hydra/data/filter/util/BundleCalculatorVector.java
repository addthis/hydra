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

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueBytes;
import com.addthis.bundle.value.ValueCustom;
import com.addthis.bundle.value.ValueDouble;
import com.addthis.bundle.value.ValueLong;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueNumber;
import com.addthis.bundle.value.ValueString;
import com.addthis.bundle.value.ValueTranslationException;

/**
 * This singleton class is used to signal on the
 * {@link com.addthis.hydra.data.filter.util.BundleCalculator}
 * value stack that the immediately following operation
 * should be a vector operation. This class throws
 * IllegalStateException if any attempt is made to read a value from
 * the object.
 */
class BundleCalculatorVector implements ValueNumber {

    private static final BundleCalculatorVector singleton = new BundleCalculatorVector();

    public static BundleCalculatorVector getSingleton() {
        return singleton;
    }

    private BundleCalculatorVector() {}

    @Override
    public ValueNumber sum(ValueNumber val) {
        throw new IllegalStateException();
    }

    @Override
    public ValueNumber diff(ValueNumber val) {
        throw new IllegalStateException();
    }

    @Override
    public ValueNumber avg(int count) {
        throw new IllegalStateException();
    }

    @Override
    public ValueNumber min(ValueNumber val) {
        throw new IllegalStateException();
    }

    @Override
    public ValueNumber max(ValueNumber val) {
        throw new IllegalStateException();
    }

    @Override
    public TYPE getObjectType() {
        return TYPE.CUSTOM;
    }

    @Override
    public ValueBytes asBytes() throws ValueTranslationException {
        throw new IllegalStateException();
    }

    @Override
    public ValueArray asArray() throws ValueTranslationException {
        throw new IllegalStateException();
    }

    @Override
    public ValueMap asMap() throws ValueTranslationException {
        throw new IllegalStateException();
    }

    @Override
    public ValueNumber asNumber() throws ValueTranslationException {
        throw new IllegalStateException();
    }

    @Override
    public ValueLong asLong() {
        throw new IllegalStateException();
    }

    @Override
    public ValueDouble asDouble() {
        throw new IllegalStateException();
    }

    @Override
    public ValueString asString() throws ValueTranslationException {
        throw new IllegalStateException();
    }

    @Override
    public ValueCustom asCustom() throws ValueTranslationException {
        throw new IllegalStateException();
    }
}
