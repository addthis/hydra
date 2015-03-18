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

import java.net.UnknownHostException;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestValueFilterFetchOnce {


    @Test
    public void fetchUrl() {
        ValueFilterFetchOnce fetch = new ValueFilterFetchOnce(5000);
        String result1 = fetch.filter("http://www.addthis.com");
        assertNotNull(result1);
        String result2 = fetch.filter("http://www.addthis.com");
        assertEquals(result1, result2);
    }

    @Test(expected=IllegalStateException.class)
    public void fetchOnce() {
        ValueFilterFetchOnce fetch = new ValueFilterFetchOnce(5000);
        fetch.filter("http://www.addthis.com");
        fetch.filter("http://blog.addthis.com");
    }

    @Test(expected=NullPointerException.class)
    public void nullInput() {
        ValueFilterFetchOnce fetch = new ValueFilterFetchOnce(5000);
        fetch.filter((String) null);
    }

    @Test
    public void fetchError() {
        boolean error = false;
        ValueFilterFetchOnce fetch = new ValueFilterFetchOnce(5000);
        try {
            fetch.filter("http://wwdwdkjwdkj.wwdwdwddwwd.wwwawdwadwad");
        } catch (Exception ex) {
            error = true;
            assertTrue(ex.getCause() instanceof UnknownHostException);
        }
        assertTrue(error);
    }

}
