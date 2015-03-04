package com.addthis.hydra.task.source;

import java.io.IOException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.config.Configs;

import com.google.common.collect.Lists;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DataSourceConstantTest {

    private Bundle bundle;
    private DataSourceConstant source;

    @Test
    public void noRepeat() throws IOException {
        source = Configs.decodeObject(DataSourceConstant.class, "bundles:[{A:a,B:1},{C:[1,2],D:[a,b]}]");
        verifyOnePass();

        // no more
        assertNull(source.next());
    }

    @Test
    public void repeatTwice() throws IOException {
        source = Configs.decodeObject(DataSourceConstant.class, "bundles:[{A:a,B:1},{C:[1,2],D:[a,b]}], repeat:2");

        // 3 passes
        verifyOnePass();
        verifyOnePass();
        verifyOnePass();

        // no more
        assertNull(source.next());
    }

    @Test
    public void repeatForever() throws IOException {
        source = Configs.decodeObject(DataSourceConstant.class, "bundles:[{A:a,B:1},{C:[1,2],D:[a,b]}], repeat:-1");

        // 3 passes
        for (int i = 0; i < 10; i++) {
            verifyOnePass();
        }
        source.close();

        // no more
        assertNull(source.next());
    }

    @Test
    public void emptyBundles() throws IOException {
        source = Configs.decodeObject(DataSourceConstant.class, "bundles:[]");
        // no more
        assertNull(source.next());
    }

    private ValueObject getField(String field) {
        return bundle.getValue(bundle.getFormat().getField(field));
    }

    private void verifyOnePass() {
        bundle = source.next();
        assertNotNull(bundle);
        assertEquals("a", getField("A").asString().asNative());
        assertEquals(new Long(1), getField("B").asLong().asNative());
        assertNotNull(bundle);

        bundle = source.next();
        assertNotNull(bundle);
        assertEquals(Lists.newArrayList(new Long(1), new Long(2)), getField("C").asArray().asNative());
        assertEquals(Lists.newArrayList("a", "b"), getField("D").asArray().asNative());
    }

}