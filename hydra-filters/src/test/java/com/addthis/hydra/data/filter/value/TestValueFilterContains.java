package com.addthis.hydra.data.filter.value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.addthis.bundle.value.ValueFactory;
import com.addthis.bundle.value.ValueObject;

import org.apache.commons.lang.RandomStringUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestValueFilterContains {

    @Test
    public void scalarInput() {
        String[] values = new String[3];
        values[0] = "foo";
        values[1] = "bar";
        values[2] = "baz";
        ValueFilterContains filter = new ValueFilterContains(values, null, false, false);
        assertNotNull(filter.filter(ValueFactory.create("foo")));
        assertNotNull(filter.filter(ValueFactory.create("bar")));
        assertNotNull(filter.filter(ValueFactory.create("baz")));
        assertNotNull(filter.filter(ValueFactory.create("hellofooworld")));
        assertNull(filter.filter(ValueFactory.create("")));
        assertNull(filter.filter(null));
    }

    @Test
    public void integerInput() {
        String[] values = new String[3];
        values[0] = "200";
        values[1] = "300";
        values[2] = "400";
        ValueFilterContains filter = new ValueFilterContains(values, null, false, false);
        assertNotNull(filter.filter(ValueFactory.create(200)));
        assertNotNull(filter.filter(ValueFactory.create(300)));
        assertNotNull(filter.filter(ValueFactory.create(400)));
        assertNotNull(filter.filter(ValueFactory.create(2000)));
        assertNull(filter.filter(ValueFactory.create(5)));
        assertNull(filter.filter(null));
    }

    @Test
    public void escapedInput() {
        String[] values = new String[3];
        values[0] = "foo";
        values[1] = "bar\\Ebar";
        values[2] = "baz";
        ValueFilterContains filter = new ValueFilterContains(values, null, false, false);
        assertNotNull(filter.filter(ValueFactory.create("foo")));
        assertNotNull(filter.filter(ValueFactory.create("bar\\Ebar")));
        assertNotNull(filter.filter(ValueFactory.create("baz")));
        assertNotNull(filter.filter(ValueFactory.create("hellofooworld")));
        assertNull(filter.filter(ValueFactory.create("")));
        assertNull(filter.filter(null));
    }


    @Test
    public void returnMatch() {
        String[] values = new String[3];
        values[0] = "foo";
        values[1] = "bar";
        values[2] = "baz";
        ValueFilterContains filter = new ValueFilterContains(values, null, false, true);
        assertEquals("foo", filter.filter(ValueFactory.create("foo")).asString().toString());
        assertEquals("bar", filter.filter(ValueFactory.create("bar")).asString().toString());
        assertEquals("baz", filter.filter(ValueFactory.create("baz")).asString().toString());
        assertEquals("foo", filter.filter(ValueFactory.create("hellofooworld")).asString().toString());
        assertNull(filter.filter(ValueFactory.create("")));
        assertNull(filter.filter(null));
    }


    private static class SearchThread extends Thread {

        final Set<String> needles;
        final List<String> haystack;
        final ValueFilterContains filter;

        SearchThread(Set<String> needles, int numHaystack, ValueFilterContains filter) {
            this.needles = needles;
            this.filter = filter;
            this.haystack = new ArrayList<>(needles);
            for (int i = needles.size(); i < numHaystack; i++) {
                String candidate = RandomStringUtils.randomAscii(20);
                if (!needles.contains(candidate)) {
                    haystack.add(candidate);
                }
            }
            Collections.shuffle(haystack);
        }

        @Override
        public void start() {
            int count = 0;
            for (String next : haystack) {
                ValueObject result = filter.filterValue(ValueFactory.create(next));
                if (needles.contains(next)) {
                    assertEquals(next, result.asString().toString());
                    count++;
                } else {
                    assertNull(result);
                }
            }
            assertEquals(needles.size(), count);
        }

    }

    @Test
    public void multithreadedTest() throws InterruptedException {
        int numThreads = 8;
        int numNeedles = 100;
        int numHaystack = 1000;
        SearchThread[] threads = new SearchThread[numThreads];

        Set<String> needles = new HashSet<>();
        for (int i = 0; i < numNeedles; i++) {
            needles.add(RandomStringUtils.randomAscii(20));
        }

        ValueFilterContains filter = new ValueFilterContains(needles.toArray(new String[numNeedles]), null, false, false);

        for (int i = 0; i < numThreads; i++) {
            threads[i] = new SearchThread(needles, numHaystack, filter);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
    }


}
