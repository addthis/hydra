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
package com.addthis.hydra.task.map;

import com.addthis.hydra.task.output.OutputStreamFlags;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class OutputFlagsTest {

    @Test
    public void testEquals_allFalse() throws Exception {
        OutputStreamFlags outputFlags = new OutputStreamFlags(0, null);
        assertEquals(null, outputFlags.getHeader());
        assertEquals(false, outputFlags.isCompress());
        assertEquals(false, outputFlags.isNoAppend());
        assertEquals(0, outputFlags.getGraceTime());
        assertEquals(0, outputFlags.getMaxFileSize());
    }

    @Test
    public void testEquals_compress() throws Exception {
        OutputStreamFlags outputFlags = new OutputStreamFlags(1, null);
        assertEquals(null, outputFlags.getHeader());
        assertEquals(true, outputFlags.isCompress());
        assertEquals(false, outputFlags.isNoAppend());
        assertEquals(0, outputFlags.getGraceTime());
        assertEquals(0, outputFlags.getMaxFileSize());
    }

    @Test
    public void testEquals_noAppend() throws Exception {
        OutputStreamFlags outputFlags = new OutputStreamFlags(4, null);
        assertEquals(null, outputFlags.getHeader());
        assertEquals(false, outputFlags.isCompress());
        assertEquals(true, outputFlags.isNoAppend());
        assertEquals(0, outputFlags.getGraceTime());
        assertEquals(0, outputFlags.getMaxFileSize());
    }

    @Test
    public void testEquals_noAppend_and_compress() throws Exception {
        OutputStreamFlags outputFlags = new OutputStreamFlags(5, null);
        assertEquals(null, outputFlags.getHeader());
        assertEquals(true, outputFlags.isCompress());
        assertEquals(true, outputFlags.isNoAppend());
        assertEquals(0, outputFlags.getGraceTime());
        assertEquals(0, outputFlags.getMaxFileSize());
    }

    @Test
    public void testEquals_allTrue() throws Exception {
        OutputStreamFlags outputFlags = new OutputStreamFlags(7, null);
        assertEquals(null, outputFlags.getHeader());
        assertEquals(true, outputFlags.isCompress());
        assertEquals(true, outputFlags.isNoAppend());
        assertEquals(0, outputFlags.getGraceTime());
        assertEquals(0, outputFlags.getMaxFileSize());
    }

    @Test
    public void testEquals_allTrue_andGraceTime() throws Exception {
        OutputStreamFlags outputFlags = new OutputStreamFlags(0xFFFFF, null);
        assertEquals(null, outputFlags.getHeader());
        assertEquals(true, outputFlags.isCompress());
        assertEquals(true, outputFlags.isNoAppend());
        assertEquals(15 * 60000l, outputFlags.getGraceTime());
        assertEquals(0, outputFlags.getMaxFileSize());
    }

    @Test
    public void testEquals_allTrue_andMaxFileSize() throws Exception {
        OutputStreamFlags outputFlags = new OutputStreamFlags(0x0F0FFFFF, null);
        assertEquals(null, outputFlags.getHeader());
        assertEquals(true, outputFlags.isCompress());
        assertEquals(true, outputFlags.isNoAppend());
        assertEquals(15 * 60000l, outputFlags.getGraceTime());
        assertEquals((15 * (1024l * 1024l)), outputFlags.getMaxFileSize());
    }
}
