package com.devsmart.mondo.kademlia;

import org.junit.Test;
import static org.junit.Assert.*;

public class IDTests {

    private static ID createShortId(int firstbyte) {
        byte[] iddata = new byte[ID.NUM_BYTES];
        iddata[0] = (byte) firstbyte;
        return new ID(iddata, 0);
    }

    @Test
    public void testIDDistanceEqual() {
        ID a = createShortId(0b1);
        ID b = createShortId(0b1);
        assertEquals(0, a.compareTo(b));
    }

    @Test
    public void testIDDistance() {
        ID a = createShortId(0b10);
        ID b = createShortId(0b1);
        assertTrue(a.compareTo(b) > 0);
    }
}
