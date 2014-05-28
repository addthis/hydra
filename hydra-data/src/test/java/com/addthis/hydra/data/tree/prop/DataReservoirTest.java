package com.addthis.hydra.data.tree.prop;

import java.util.Collection;
import java.util.Iterator;

import com.addthis.hydra.data.tree.ReadNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataReservoirTest {

    @Test
    public void testResizeReservoir() {
        DataReservoir reservoir = new DataReservoir();
        reservoir.updateReservoir(0, 4);
        reservoir.updateReservoir(1, 4);
        reservoir.updateReservoir(2, 4);
        reservoir.updateReservoir(3, 4);
        assertEquals(-2, reservoir.retrieveCount(4));
        reservoir.updateReservoir(4, 8);
        reservoir.updateReservoir(5, 8);
        assertEquals(1, reservoir.retrieveCount(4));
        assertEquals(1, reservoir.retrieveCount(5));
        assertEquals(0, reservoir.retrieveCount(6));
        assertEquals(0, reservoir.retrieveCount(7));
        assertEquals(-2, reservoir.retrieveCount(8));
        reservoir.updateReservoir(5, 4);
        assertEquals(-1, reservoir.retrieveCount(3));
        assertEquals(1, reservoir.retrieveCount(4));
        assertEquals(2, reservoir.retrieveCount(5));
        assertEquals(0, reservoir.retrieveCount(6));
        assertEquals(0, reservoir.retrieveCount(7));
        assertEquals(-2, reservoir.retrieveCount(8));
    }

    @Test
    public void testSimpleUpdateReservoir() {
        DataReservoir reservoir = new DataReservoir();
        assertEquals(-3, reservoir.retrieveCount(0));
        assertEquals(-3, reservoir.retrieveCount(10));
        assertEquals(-3, reservoir.retrieveCount(-10));
        reservoir.updateReservoir(0, 4);
        reservoir.updateReservoir(1, 4);
        reservoir.updateReservoir(2, 4);
        reservoir.updateReservoir(3, 4);
        assertEquals(1, reservoir.retrieveCount(0));
        assertEquals(1, reservoir.retrieveCount(1));
        assertEquals(1, reservoir.retrieveCount(2));
        assertEquals(1, reservoir.retrieveCount(3));
        assertEquals(-1, reservoir.retrieveCount(-1));
        assertEquals(-2, reservoir.retrieveCount(4));
        reservoir.updateReservoir(5, 4);
        assertEquals(-1, reservoir.retrieveCount(0));
        assertEquals(-1, reservoir.retrieveCount(1));
        assertEquals(1, reservoir.retrieveCount(2));
        assertEquals(1, reservoir.retrieveCount(3));
        assertEquals(0, reservoir.retrieveCount(4));
        assertEquals(1, reservoir.retrieveCount(5));
        assertEquals(-2, reservoir.retrieveCount(6));
        reservoir.updateReservoir(9, 4);
        assertEquals(-1, reservoir.retrieveCount(5));
        assertEquals(0, reservoir.retrieveCount(6));
        assertEquals(0, reservoir.retrieveCount(7));
        assertEquals(0, reservoir.retrieveCount(8));
        assertEquals(1, reservoir.retrieveCount(9));
        assertEquals(-2, reservoir.retrieveCount(10));
    }

    @Test
    public void testCompoundUpdateReservoir() {
        DataReservoir reservoir = new DataReservoir();
        reservoir.updateReservoir(1, 4, 4);
        reservoir.updateReservoir(2, 4, 8);
        reservoir.updateReservoir(3, 4, 4);
        reservoir.updateReservoir(4, 4, 4);
        assertEquals(4, reservoir.retrieveCount(1));
        assertEquals(8, reservoir.retrieveCount(2));
        assertEquals(4, reservoir.retrieveCount(3));
        assertEquals(4, reservoir.retrieveCount(4));
    }

    @Test
    public void testGetNodesBadInput() {
        DataReservoir reservoir = new DataReservoir();
        reservoir.updateReservoir(1, 4, 4);
        reservoir.updateReservoir(2, 4, 12);
        reservoir.updateReservoir(3, 4, 4);
        reservoir.updateReservoir(4, 4, 100);
        Collection<ReadNode> result = reservoir.getNodes(null, "epoch=4~sigma=2.0");
        assertEquals(0, result.size());
        result = reservoir.getNodes(null, "epoch=4~obs=3");
        assertEquals(0, result.size());
        result = reservoir.getNodes(null, "sigma=2.0~obs=3");
        assertEquals(0, result.size());
        result = reservoir.getNodes(null, "epoch=4~sigma=2.0~obs=4");
        assertEquals(0, result.size());
        result = reservoir.getNodes(null, "epoch=4~sigma=2.0~obs=3");
        assertEquals(1, result.size());
    }

    @Test
    public void testGetNodes() {
        DataReservoir reservoir = new DataReservoir();
        reservoir.updateReservoir(1, 4, 4);
        reservoir.updateReservoir(2, 4, 12);
        reservoir.updateReservoir(3, 4, 4);
        reservoir.updateReservoir(4, 4, 100);
        Collection<ReadNode> result = reservoir.getNodes(null, "epoch=4~sigma=2.0~obs=3");
        assertEquals(1, result.size());
        ReadNode node = result.iterator().next();
        assertEquals(node.getName(), "delta");
        assertEquals(86, node.getCounter());
        node = node.getIterator().next();
        assertEquals(node.getName(), "measurement");
        assertEquals(100, node.getCounter());
        node = node.getIterator().next();
        assertEquals(node.getName(), "mean");
        assertEquals(7, node.getCounter());
        node = node.getIterator().next();
        assertEquals(node.getName(), "stddev");
        assertEquals(4, node.getCounter());
        node = node.getIterator().next();
        assertEquals(node.getName(), "threshold");
        assertEquals(14, node.getCounter());
    }

    @Test
    public void testGetNodesWithMin() {
        DataReservoir reservoir = new DataReservoir();
        reservoir.updateReservoir(1, 4, 4);
        reservoir.updateReservoir(2, 4, 12);
        reservoir.updateReservoir(3, 4, 4);
        reservoir.updateReservoir(4, 4, 100);
        Collection<ReadNode> result = reservoir.getNodes(null, "epoch=4~sigma=2.0~obs=3~min=101");
        assertEquals(0, result.size());
        reservoir.updateReservoir(4, 4);
        result = reservoir.getNodes(null, "epoch=4~sigma=2.0~obs=3~min=101");
        assertEquals(1, result.size());
    }

    @Test
    public void testGetNodesWithRaw() {
        DataReservoir reservoir = new DataReservoir();
        reservoir.updateReservoir(1, 4, 4);
        reservoir.updateReservoir(2, 4, 12);
        reservoir.updateReservoir(3, 4, 4);
        reservoir.updateReservoir(4, 4, 100);
        Collection<ReadNode> result = reservoir.getNodes(null, "epoch=4~sigma=2.0~obs=3~raw=true");
        assertEquals(3, result.size());
        for(ReadNode node : result) {
            switch (node.getName()) {
                case "delta":
                    break;
                case "minEpoch":
                    assertEquals(1, node.getCounter());
                    break;
                case "observations": {
                    Iterator<? extends ReadNode> children = node.getIterator();
                    ReadNode child;
                    assertTrue(children.hasNext());
                    child = children.next();
                    assertEquals("1", child.getName());
                    assertEquals(4, child.getCounter());
                    assertTrue(children.hasNext());
                    child = children.next();
                    assertEquals("2", child.getName());
                    assertEquals(12, child.getCounter());
                    assertTrue(children.hasNext());
                    child = children.next();
                    assertEquals("3", child.getName());
                    assertEquals(4, child.getCounter());
                    assertTrue(children.hasNext());
                    child = children.next();
                    assertEquals("4", child.getName());
                    assertEquals(100, child.getCounter());
                    assertFalse(children.hasNext());
                    break;
                }
                default:
                    fail("Unexpected node " + node.getName());
            }
        }
    }

    @Test
    public void testSerialization() {
        DataReservoir reservoir = new DataReservoir();
        byte[] encoding = reservoir.bytesEncode(0);
        assertEquals(0, encoding.length);
        reservoir.bytesDecode(encoding, 0);
        assertEquals(-3, reservoir.retrieveCount(0));
        reservoir.updateReservoir(0, 4);
        reservoir.updateReservoir(1, 4);
        reservoir.updateReservoir(2, 4);
        reservoir.updateReservoir(3, 4);
        encoding = reservoir.bytesEncode(0);
        reservoir = new DataReservoir();
        reservoir.bytesDecode(encoding, 0);
        assertEquals(1, reservoir.retrieveCount(0));
        assertEquals(1, reservoir.retrieveCount(1));
        assertEquals(1, reservoir.retrieveCount(2));
        assertEquals(1, reservoir.retrieveCount(3));
        assertEquals(-1, reservoir.retrieveCount(-1));
        assertEquals(-2, reservoir.retrieveCount(4));
    }

}
