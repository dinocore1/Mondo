package com.devsmart.mondo.data;


import org.junit.Test;
import static org.junit.Assert.*;

public class BuzhashTest {


    @Test
    public void buzhashRoll() {
        byte[] data = new byte[] {0, 1, 1, 3};

        Buzhash hash = new Buzhash(1);

        long h1 = hash.addByte(data[0]);
        long h2 = hash.roll(data[0], data[1]);
        long h3 = hash.roll(data[1], data[2]);
        long h4 = hash.roll(data[2], data[3]);

        assertEquals(h2, h3);
        assertNotEquals(h3, h4);
        assertNotEquals(h3, h1);
    }
}
