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
package com.addthis.hydra.task.output;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import java.lang.reflect.Method;

import com.addthis.basis.test.SlowTest;

import com.addthis.codec.config.Configs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(SlowTest.class)
public class DefaultOutputWrapperFactoryTest {

    private File tmpDir;
    private DefaultOutputWrapperFactory localWriteStream;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        tmpDir = testFolder.newFolder();
        localWriteStream = new DefaultOutputWrapperFactory(tmpDir.getCanonicalPath());
    }

    @Test
    public void testGetFileName() throws Exception {
        Class[] parameterTypes = new Class[4];
        parameterTypes[0] = String.class;
        parameterTypes[1] = PartitionData.class;
        parameterTypes[2] = OutputStreamFlags.class;
        parameterTypes[3] = int.class;

        Method m = localWriteStream.getClass().getDeclaredMethod("getFileName", parameterTypes);
        m.setAccessible(true);

        Object[] parameters = new Object[4];
        parameters[0] = "foo";
        parameters[1] = new PartitionData(null, 3);
        parameters[2] = Configs.decodeObject(OutputStreamFlags.class, "compressType:GZIP, compress:true");
        parameters[3] = 0;

        String result = (String) m.invoke(localWriteStream, parameters);
        assertNotNull(result);
        assertEquals("foo.gz", result);

    }

    @Test
    public void testGetFileName_noCompress() throws Exception {
        Class[] parameterTypes = new Class[4];
        parameterTypes[0] = String.class;
        parameterTypes[1] = PartitionData.class;
        parameterTypes[2] = OutputStreamFlags.class;
        parameterTypes[3] = int.class;

        Method m = localWriteStream.getClass().getDeclaredMethod("getFileName", parameterTypes);
        m.setAccessible(true);

        Object[] parameters = new Object[4];
        parameters[0] = "foo";
        parameters[1] = new PartitionData(null, 3);
        parameters[2] = new OutputStreamFlags(0);
        parameters[3] = 0;

        String result = (String) m.invoke(localWriteStream, parameters);
        assertNotNull(result);
        assertEquals("foo", result);

    }

    @Test
    public void testGetFileName_noAppend() throws Exception {
        Class[] parameterTypes = new Class[4];
        parameterTypes[0] = String.class;
        parameterTypes[1] = PartitionData.class;
        parameterTypes[2] = OutputStreamFlags.class;
        parameterTypes[3] = int.class;

        Method m = localWriteStream.getClass().getDeclaredMethod("getFileName", parameterTypes);
        m.setAccessible(true);

        Object[] parameters = new Object[4];
        parameters[0] = "foo";
        parameters[1] = new PartitionData(null, 3);
        parameters[2] = new OutputStreamFlags(4);
        parameters[3] = 0;

        String result = (String) m.invoke(localWriteStream, parameters);
        assertNotNull(result);
        assertEquals("foo-000", result);

    }

    @Test
    public void testGetFileName_noAppend_withCompress() throws Exception {
        Class[] parameterTypes = new Class[4];
        parameterTypes[0] = String.class;
        parameterTypes[1] = PartitionData.class;
        parameterTypes[2] = OutputStreamFlags.class;
        parameterTypes[3] = int.class;

        Method m = localWriteStream.getClass().getDeclaredMethod("getFileName", parameterTypes);
        m.setAccessible(true);

        Object[] parameters = new Object[4];
        parameters[0] = "foo";
        parameters[1] = new PartitionData(null, 3);
        parameters[2] =
                Configs.decodeObject(OutputStreamFlags.class, "compressType:GZIP, compress:true, noAppend:true");
        parameters[3] = 0;

        String result = (String) m.invoke(localWriteStream, parameters);
        assertNotNull(result);
        assertEquals("foo-000.gz", result);

    }

    @Test
    public void testGetFileName_noAppend_withCompress_withExistingFile() throws Exception {
        // create dummy file
        File tmpFile = new File(tmpDir, "foo-000.gz");
        PrintWriter pw = new PrintWriter(tmpFile);
        for (int i = 0; i < 100; i++) {
            pw.append("a few dummy lines of data: row").append(String.valueOf(i));
        }
        pw.flush();
        pw.close();

        Class[] parameterTypes = new Class[4];
        parameterTypes[0] = String.class;
        parameterTypes[1] = PartitionData.class;
        parameterTypes[2] = OutputStreamFlags.class;
        parameterTypes[3] = int.class;

        Method m = localWriteStream.getClass().getDeclaredMethod("getFileName", parameterTypes);
        m.setAccessible(true);

        Object[] parameters = new Object[4];
        parameters[0] = "foo";
        parameters[1] = new PartitionData(null, 3);
        parameters[2] =
                Configs.decodeObject(OutputStreamFlags.class, "compressType:GZIP, compress:true, noAppend:true");
        parameters[3] = 1;

        String result = (String) m.invoke(localWriteStream, parameters);
        assertNotNull(result);
        assertEquals("foo-001.gz", result);

    }

    @Test
    public void testGetFileName_withExistingFile() throws Exception {
        // create dummy file
        File tmpFile = null;
        tmpFile = new File(tmpDir + "/foo-000");
        PrintWriter pw = new PrintWriter(tmpFile);
        for (int i = 0; i < 100; i++) {
            pw.append("a few dummy lines of data: row").append(String.valueOf(i));
        }
        pw.flush();
        pw.close();

        Class[] parameterTypes = new Class[4];
        parameterTypes[0] = String.class;
        parameterTypes[1] = PartitionData.class;
        parameterTypes[2] = OutputStreamFlags.class;
        parameterTypes[3] = int.class;

        Method m = localWriteStream.getClass().getDeclaredMethod("getFileName", parameterTypes);
        m.setAccessible(true);

        Object[] parameters = new Object[4];
        parameters[0] = "foo";
        parameters[1] = new PartitionData(null, 3);
        parameters[2] = new OutputStreamFlags(0x0F0FFF00);
        parameters[3] = 0;

        String result = (String) m.invoke(localWriteStream, parameters);
        assertNotNull(result);
        assertEquals("foo-000", result);
    }

    @Test
    public void testGetFileName_withExistingFile_exceedsMax() throws Exception {
        // create dummy file
        File tmpFile = null;
        tmpFile = new File(tmpDir + "/foo-000");
        PrintWriter pw = new PrintWriter(tmpFile);
        for (int i = 0; i < (1024l * 1024l); i++) {
            pw.append("a few dummy lines of data: row").append(String.valueOf(i));
        }
        pw.flush();
        pw.close();

        Class[] parameterTypes = new Class[4];
        parameterTypes[0] = String.class;
        parameterTypes[1] = PartitionData.class;
        parameterTypes[2] = OutputStreamFlags.class;
        parameterTypes[3] = int.class;

        Method m = localWriteStream.getClass().getDeclaredMethod("getFileName", parameterTypes);
        m.setAccessible(true);

        Object[] parameters = new Object[4];
        parameters[0] = "foo";
        parameters[1] = new PartitionData(null, 3);
        parameters[2] = new OutputStreamFlags(0x0F01FF00);
        parameters[3] = 1;

        String result = (String) m.invoke(localWriteStream, parameters);
        assertNotNull(result);
        assertEquals("foo-001", result);
    }
}
