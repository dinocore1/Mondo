package com.devsmart.mondo.data;


import com.devsmart.mondo.storage.VirtualFile;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VirtualFileTest {

    @Test
    public void testGetNumBlocks() throws Exception {

        assertEquals(0, VirtualFile.getNumBlocks(0, 4096));
        assertEquals(1, VirtualFile.getNumBlocks(1, 4096));
        assertEquals(1, VirtualFile.getNumBlocks(4095, 4096));
        assertEquals(1, VirtualFile.getNumBlocks(4096, 4096));
        assertEquals(2, VirtualFile.getNumBlocks(4097, 4096));

    }
}
