package com.devsmart.mondo.data;


import com.google.common.collect.Iterables;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockSetTest {


    @Test
    public void testInsertOverlapping() {

        BlockSet<Void> blockSet = new BlockSet<Void>();
        blockSet.add(new Block<Void>(0, 10));
        assertEquals(1, blockSet.size());
        assertEquals(new Block<Void>(0, 10, 0, null), Iterables.get(blockSet, 0));

        blockSet.add(new Block<Void>(10, 10, 0, null));
        assertEquals(2, blockSet.size());
        assertEquals(new Block<Void>(0, 10, 0, null), Iterables.get(blockSet, 0));
        assertEquals(new Block<Void>(10, 10, 0, null), Iterables.get(blockSet, 1));

        blockSet.add(new Block<Void>(5, 10));
        assertEquals(3, blockSet.size());

        assertEquals(new Block<Void>(0, 5, 0, null), Iterables.get(blockSet, 0));
        assertEquals(new Block<Void>(5, 10, 0, null), Iterables.get(blockSet, 1));
        assertEquals(new Block<Void>(15, 5, 5, null), Iterables.get(blockSet, 2));

    }

    @Test
    public void testGetBlock() {
        Block<Void> b;

        BlockSet<Void> blockSet = new BlockSet<Void>();
        blockSet.add(new Block<Void>(0, 10));
        blockSet.add(new Block<Void>(10, 10));
        blockSet.add(new Block<Void>(20, 10));

        b = blockSet.getBlockContaining(0);
        assertNotNull(b);
        assertEquals(new Block<Void>(0, 10), b);

        b = blockSet.getBlockContaining(5);
        assertNotNull(b);
        assertEquals(new Block<Void>(0, 10), b);

        b = blockSet.getBlockContaining(10);
        assertNotNull(b);
        assertEquals(new Block<Void>(10, 10), b);

        b = blockSet.getBlockContaining(15);
        assertNotNull(b);
        assertEquals(new Block<Void>(10, 10), b);

        b = blockSet.getBlockContaining(20);
        assertNotNull(b);
        assertEquals(new Block<Void>(20, 10), b);

        b = blockSet.getBlockContaining(29);
        assertNotNull(b);
        assertEquals(new Block<Void>(20, 10), b);

        b = blockSet.getBlockContaining(30);
        assertNull(b);

        b = blockSet.getBlockContaining(35);
        assertNull(b);
    }
}
