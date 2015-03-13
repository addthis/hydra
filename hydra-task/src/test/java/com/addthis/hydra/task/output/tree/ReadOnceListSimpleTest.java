package com.addthis.hydra.task.output.tree;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadOnceListSimpleTest {

    private static class NoopReadOnceList extends ReadOnceListSimple<String> {
        @Override protected void doRelease() {}
    }

    @Test
    public void read() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.add("foo");
        list.add("bar");
        list.add("baz");
        assertFalse(list.isRead());
        assertFalse(list.isReleased());
        list.iterator();
        assertTrue(list.isRead());
        assertFalse(list.isReleased());
    }

    @Test
    public void release() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.add("foo");
        list.add("bar");
        list.add("baz");
        assertFalse(list.isRead());
        assertFalse(list.isReleased());
        list.release();
        assertFalse(list.isRead());
        assertTrue(list.isReleased());
    }

    @Test
    public void head() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.add("foo");
        list.add("bar");
        list.add("baz");
        assertFalse(list.isRead());
        assertFalse(list.isReleased());
        assertEquals("foo", list.head());
        assertTrue(list.isRead());
        assertFalse(list.isReleased());
    }

    @Test
    public void transferOwnership() {
        NoopReadOnceList list = new NoopReadOnceList();
        NoopReadOnceList list2 = new NoopReadOnceList();
        list.add("foo");
        list.add("bar");
        list.add("baz");
        assertFalse(list.isRead());
        assertFalse(list.isReleased());
        list2.addAll(list);
        assertTrue(list.isRead());
        assertFalse(list.isReleased());
        assertFalse(list2.isRead());
        assertFalse(list2.isReleased());
    }

    @Test(expected = IllegalStateException.class)
    public void readTwice() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.iterator();
        list.iterator();
    }

    @Test(expected = IllegalStateException.class)
    public void releaseTwice() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.release();
        list.release();
    }

    @Test(expected = IllegalStateException.class)
    public void readAndRelease() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.iterator();
        list.release();
    }

    @Test(expected = IllegalStateException.class)
    public void releaseAndRead() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.release();
        list.iterator();
    }

    @Test(expected = IllegalStateException.class)
    public void releaseAndAdd() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.release();
        list.add("foo");
    }

    @Test(expected = IllegalStateException.class)
    public void readAndAdd() {
        NoopReadOnceList list = new NoopReadOnceList();
        list.iterator();
        list.add("foo");
    }

}
