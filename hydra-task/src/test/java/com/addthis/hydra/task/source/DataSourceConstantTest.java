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
package com.addthis.hydra.task.source;

import javax.annotation.Nonnull;
import javax.annotation.Syntax;

import java.io.IOException;

import java.util.NoSuchElementException;

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.config.Configs;

import com.google.common.collect.Lists;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DataSourceConstantTest {

    private Bundle bundle;
    private DataSourceConstant source;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void noRepeat() throws IOException {
        source = initSource("bundles:[{A:a,B:1},{C:[1,2],D:[a,b]}]");

        verifyOnePass();

        // no more
        verifyNoMoreBundles();
    }

    @Test
    public void repeatTwice() throws IOException {
        source = initSource("bundles:[{A:a,B:1},{C:[1,2],D:[a,b]}], repeat:2");

        // 3 passes
        verifyOnePass();
        verifyOnePass();
        verifyOnePass();

        verifyNoMoreBundles();
    }

    @Test
    public void repeatForever() throws IOException {
        source = initSource("bundles:[{A:a,B:1},{C:[1,2],D:[a,b]}], repeat:-1");

        // many passes
        for (int i = 0; i < 10; i++) {
            verifyOnePass();
        }
        source.close();

        // no more
        verifyNoMoreBundles();
    }

    @Test
    public void emptyBundles() throws IOException {
        source = initSource("bundles:[]");
        verifyNoMoreBundles();
    }

    private DataSourceConstant initSource(@Syntax("HOCON") @Nonnull String config) throws IOException {
        DataSourceConstant src = Configs.decodeObject(DataSourceConstant.class, config);
        src.init();
        return src;
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

    private void verifyNoMoreBundles() {
        exception.expect(NoSuchElementException.class);
        source.next();
    }

}