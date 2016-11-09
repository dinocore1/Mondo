package com.devsmart.mondo.data;


import com.google.common.collect.Iterables;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockSetTest {


    @Test
    public void testInsertOverlapping() {

        BlockSet blockSet = new BlockSet();
        blockSet.add(new Block(0, 10));
        assertEquals(1, blockSet.mBlockSet.size());
        assertEquals(0, Iterables.get(blockSet.mBlockSet, 0).offset);

        blockSet.add(new Block(10, 10));
        assertEquals(2, blockSet.mBlockSet.size());
        assertEquals(0, Iterables.get(blockSet.mBlockSet, 0).offset);
        assertEquals(10, Iterables.get(blockSet.mBlockSet, 1).offset);

        blockSet.add(new Block(5, 10));
        assertEquals(3, blockSet.mBlockSet.size());

        assertEquals(new Block(0, 5, 0), Iterables.get(blockSet.mBlockSet, 0));
        assertEquals(new Block(5, 10, 0), Iterables.get(blockSet.mBlockSet, 1));
        assertEquals(new Block(15, 5, 5), Iterables.get(blockSet.mBlockSet, 2));

    }
}
