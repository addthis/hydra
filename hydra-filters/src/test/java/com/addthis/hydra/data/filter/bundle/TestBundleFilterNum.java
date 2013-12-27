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


import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBundleFilterNum extends TestBundleFilter {

    @Test
    public void testAdd() {
        BundleFilterNum bfn = new BundleFilterNum().setDefine("c0,n3,add,v1,set");
        Bundle bundle = new ListBundle();
        bundle.setValue(bundle.getFormat().getField("c0"), ValueFactory.create(3));
        bundle.setValue(bundle.getFormat().getField("c1"), ValueFactory.create(4));
        bfn.filter(bundle);
        assertEquals(bundle.getValue(bundle.getFormat().getField("c1")).toString(), "6");
    }

    @Test
    public void testMult() {
        BundleFilterNum bfn = new BundleFilterNum().setDefine("c0,n3,*,v1,set");
        Bundle bundle = new ListBundle();
        bundle.setValue(bundle.getFormat().getField("c0"), ValueFactory.create(3));
        bundle.setValue(bundle.getFormat().getField("c1"), ValueFactory.create(4));
        bfn.filter(bundle);
        assertEquals("9", bundle.getValue(bundle.getFormat().getField("c1")).toString());
    }

    @Test
    public void testMean() {
        BundleFilterNum bfn = new BundleFilterNum().setDefine("n2:3:5:7:11:13:17:19,mean,v0,set");
        Bundle bundle = new ListBundle();
        bundle.setValue(bundle.getFormat().getField("c1"), ValueFactory.create(-1));
        bfn.filter(bundle);
        assertEquals("9.625", bundle.getValue(bundle.getFormat().getField("c1")).toString());
    }

    @Test
    public void testVariance() {
        BundleFilterNum bfn = new BundleFilterNum().setDefine("n2:3:5:7:11:13:17:19,variance,v0,set");
        Bundle bundle = new ListBundle();
        bundle.setValue(bundle.getFormat().getField("c1"), ValueFactory.create(-1));
        bfn.filter(bundle);
        assertEquals("35.734375", bundle.getValue(bundle.getFormat().getField("c1")).toString());
    }

}
