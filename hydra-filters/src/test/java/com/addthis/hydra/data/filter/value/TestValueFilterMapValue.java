package com.addthis.hydra.data.filter.value;

/**
 * Created by steve on 3/20/17.
 */

import com.addthis.bundle.core.Bundle;
import com.addthis.bundle.core.Bundles;
import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueMap;
import com.addthis.bundle.value.ValueObject;
import com.addthis.codec.config.Configs;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class TestValueFilterMapValue {
    private static final Logger log = LoggerFactory.getLogger(TestValueFilterMapValue.class);

    public static Bundle defaultBundle() {
        try {
            return Bundles.decode("{params: {a: 1, b: 2, c: \"test\"}, somekey: b, somevalue: 10}");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ValueObject applyFilter(String filterText) throws Exception {
        Bundle bundle = defaultBundle();
        ValueFilterMapValue filter = Configs.decodeObject(ValueFilterMapValue.class, filterText);
        return filter.filterValue(bundle.getValue("params"), bundle);
    }

    @Test
    public void getInt() throws Exception {
        ValueObject value = applyFilter("{key: a}");
        assertEquals(value.asLong().asNative().longValue(), 1);
    }

    @Test
    public void getNull() throws Exception {
        ValueObject value = applyFilter("{key: d}");
        assertNull(value);
    }

    @Test
    public void getField() throws Exception {
        ValueObject value = applyFilter("{key.field: somekey}");
        assertEquals(value.asLong().asNative().longValue(), 2);
    }

    @Test
    public void getString() throws Exception {
        ValueObject value = applyFilter("{key: c}");
        assertEquals(value.asString().asNative(), "test");
    }

    @Test
    public void defaultValue() throws Exception {
        ValueObject value = applyFilter("{key: e, defaultValue: 5}");
        assertEquals(value.asLong().asNative().longValue(), 5);
    }

    @Test
    public void defaultField() throws Exception {
        ValueObject value = applyFilter("{key: e, defaultValue.field: somevalue}");
        assertEquals(value.asLong().asNative().longValue(), 10);
    }

    @Test
    public void put() throws Exception {
        ValueObject value = applyFilter("{key: z, put: 0}");
        ValueMap result = ValueFactory.createMap();
        result.put("a", ValueFactory.create(1));
        result.put("b", ValueFactory.create(2));
        result.put("c", ValueFactory.create("test"));
        result.put("z", ValueFactory.create(0));
        assertEquals(value, result);
    }

    @Test
    public void putField() throws Exception {
        ValueObject value = applyFilter("{key.field: somekey, put.field: somevalue}");
        ValueMap result = ValueFactory.createMap();
        result.put("a", ValueFactory.create(1));
        result.put("b", ValueFactory.create(10));
        result.put("c", ValueFactory.create("test"));
        assertEquals(value, result);
    }
}
