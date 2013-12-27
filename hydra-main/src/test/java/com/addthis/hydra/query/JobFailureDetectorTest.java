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
package com.addthis.hydra.query;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobFailureDetectorTest {

    @Test
    public void testInit() {
        JobFailureDetector jfd = new JobFailureDetector();
    }


    @Test
    public void testFail1() {
        JobFailureDetector jfd = new JobFailureDetector();
        jfd.indicateFailure("foo");
        jfd.indicateFailure("foo");
        assertTrue(jfd.hasFailed("foo", 1));
        assertFalse(jfd.hasFailed("foo", 3));
    }

    @Test
    public void testSuccess() {
        JobFailureDetector jfd = new JobFailureDetector();
        assertFalse(jfd.hasFailed("foo", 1));
        jfd.indicateFailure("foo");
        jfd.indicateFailure("foo");
        assertTrue(jfd.hasFailed("foo", 1));
        jfd.indicateSuccess("foo");
        assertFalse(jfd.hasFailed("foo", 1));
    }


}