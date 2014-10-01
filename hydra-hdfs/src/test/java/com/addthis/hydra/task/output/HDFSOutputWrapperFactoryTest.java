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

import java.io.ByteArrayOutputStream;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.codec.config.Configs;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class HDFSOutputWrapperFactoryTest {

    @Test
    public void openWriteStream() throws Exception {
        HDFSOutputWrapperFactory factory = Configs.decodeObject(
                HDFSOutputWrapperFactory.class, "hdfsUrl = \"hdfs://lhn00.clearspring.local:8020\", dir = ./test2");
        OutputStreamFlags outputFlags = new OutputStreamFlags(false, false, 1000, 100000, "hello");
        OutputStreamChannel channel = new OutputStreamChannel();
        OutputWrapper wrapper = factory.openWriteStream("test_hdfs", outputFlags, channel.createEmitter());
        ByteArrayOutputStream bufOut = new ByteArrayOutputStream();
        Bundle test = new ListBundle();
        test.setValue(test.getFormat().getField("name"), ValueFactory.create("helloworld"));
        wrapper.write(bufOut, test);
        wrapper.write(bufOut.toByteArray());
        wrapper.close();
    }
}