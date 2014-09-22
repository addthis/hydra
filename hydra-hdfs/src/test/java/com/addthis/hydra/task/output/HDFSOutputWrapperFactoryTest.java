package com.addthis.hydra.task.output;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.list.ListBundle;
import com.addthis.bundle.value.ValueFactory;

import java.io.ByteArrayOutputStream;

public class HDFSOutputWrapperFactoryTest {

    @org.junit.Test
    public void testOpenWriteStream() throws Exception {
        HDFSOutputWrapperFactory factory = new HDFSOutputWrapperFactory();
        factory.setHdfsURL("hdfs://lhn00.clearspring.local:8020");
        factory.setDir("./test2");
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