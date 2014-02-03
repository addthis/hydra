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

import java.util.Arrays;

import com.addthis.bundle.value.ValueArray;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestValueFilterJoin {

    @Test
    public void arrayTest() {
        ValueFilterJoin join = new ValueFilterJoin();
        ValueArray strArray = ValueFactory.createValueArray(Arrays.asList("b", "c", "a"));
        ValueArray numArray = ValueFactory.createArray(3);
        for (int i = 0; i < 5; i++) {
            numArray.add(ValueFactory.create(5 - i));
        }
        assertEquals("should correctly join string array", "b,c,a", join.filter(strArray).toString());
        join.setSort(true);
        assertEquals("should correctly sort and join string array", "a,b,c", join.filter(strArray).toString());
        join.setSort(false);
        assertEquals("should correctly join num array", "5,4,3,2,1", join.filter(numArray).toString());
        join.setSort(true);
        assertEquals("should correctly sort and join num array", "1,2,3,4,5", join.filter(numArray).toString());
    }

    @Test
    public void mapTest() {
        ValueFilterJoin join = new ValueFilterJoin();
        ValueMap map = ValueFactory.createMap(ImmutableMap.of(
                "b", Arrays.asList("p", "q"),
                "a", Arrays.asList("x", "y", "z"),
                "c", Arrays.asList("c", "d", "e")
        ));
        join.setJoin(";");
        assertEquals("should correctly join map", "b=p,q;c=c,d,e;a=x,y,z", join.filter(map).toString());
    }
}
