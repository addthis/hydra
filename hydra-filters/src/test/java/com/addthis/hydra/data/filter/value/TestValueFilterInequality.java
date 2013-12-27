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

import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestValueFilterInequality {

    @Test
    public void testevalExpression() {
        ValueFilterInequality vfi = new ValueFilterInequality();
        assertEquals(false, vfi.evalExpression("lasfdt", 100, 200));
        assertEquals(false, vfi.evalExpression("gteq", 5, 10));
        assertEquals(true, vfi.evalExpression("gteq", 10, 5));
        assertEquals(true, vfi.evalExpression("gt", 10, 5));
        assertEquals(true, vfi.evalExpression("lt", 100, 200));
        assertEquals(true, vfi.evalExpression("lteq", 7, 7));
        assertEquals(false, vfi.evalExpression("eq", 100, 101));
        assertEquals(true, vfi.evalExpression("eq", 137, 137));
    }

    @Test
    public void testevalExpression_double() {
        ValueFilterInequality vfi = new ValueFilterInequality();
        assertEquals(false, vfi.evalExpressionD("lasfdt", 100.2, 200.5));
        assertEquals(false, vfi.evalExpressionD("gteq", 5.34, 10.24));
        assertEquals(true, vfi.evalExpressionD("gteq", 10.05, 5.34));
        assertEquals(true, vfi.evalExpressionD("gt", 10, 5));
        assertEquals(true, vfi.evalExpressionD("lt", 100.344, 100.355));
        assertEquals(true, vfi.evalExpressionD("lteq", 7, 7));
        assertEquals(false, vfi.evalExpressionD("eq", 100, 101));
        assertEquals(true, vfi.evalExpressionD("eq", 137, 137));
    }

    @Test
    public void testFilter() {
        ValueFilterInequality vfi = new ValueFilterInequality("lt", (long) 7);
        assertEquals(ValueFactory.create(3), vfi.filter(ValueFactory.create(3)));
        assertEquals(null, vfi.filter(ValueFactory.create(9)));
        assertEquals(null, vfi.filter(null));
    }
}
