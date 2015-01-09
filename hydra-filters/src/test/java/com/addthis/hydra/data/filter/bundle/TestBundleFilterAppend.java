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

import com.addthis.bundle.util.AutoField;
import com.addthis.bundle.util.map.MapBundle;

import com.google.common.collect.Sets;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * FIXME: These depends depend on hash order
 */
public class TestBundleFilterAppend {

    @Test
    public void fieldTest() {
        BundleFilterAppend bfa = new BundleFilterAppend().setValues(Sets.newHashSet("foo")).setToField(
                AutoField.newAutoField("bar"));
        MapBundle bundle = MapBundle.createBundle(new String[]{"dog", "food"});
        bfa.filter(bundle);
        assertEquals("food", bundle.get("dog"));
        assertEquals("foo", bundle.get("bar"));
    }

    @Test
    public void fieldTest_withExistingValue() {
        BundleFilterAppend bfa = new BundleFilterAppend().setValues(Sets.newHashSet("foo")).setToField(
                AutoField.newAutoField("bar"));
        MapBundle bundle = MapBundle.createBundle(new String[]{"dog", "food", "bar", "car"});
        bfa.filter(bundle);
        assertEquals("food", bundle.get("dog"));
        assertEquals("car,foo", bundle.get("bar"));
    }

    @Test
    public void fieldTest_multipleValues() {
        BundleFilterAppend bfa = new BundleFilterAppend().setValues(Sets.newHashSet("car", "foo", "star"))
                                                         .setToField(AutoField.newAutoField("bar")).setSize(2);
        MapBundle bundle = MapBundle.createBundle(new String[]{"dog", "food"});
        bfa.filter(bundle);
        assertEquals("food", bundle.get("dog"));
        assertEquals("star,car,foo", bundle.get("bar"));
    }
}
