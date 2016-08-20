package com.devsmart.mondo.data;


import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class BuzhashTest {


    @Test
    public void buzhashRoll() {
        /*
        final int windowSize = 2;
        Buzhash hash = new Buzhash(windowSize);
        byte[] window = new byte[windowSize];
        int circle = 0;

        byte[] data = new byte[] {6, 2, 5, 1, 1, 3, 5, 3, 1, 1, 3, 5, 18};


        for(int i=0;i<data.length;i++){
            byte in = data[i];
            long h = hash.roll(window[circle], in);
            window[circle] = in;
            circle = (circle+1) % windowSize;

            if(data[i] == 1 && data[i-1] == 1) {
                hash.reset();
                Arrays.fill(window, (byte)0);
            }

            System.out.println(String.format("[%d: %d]", i, h));
        }


        long h1 = hash.addByte(data[0]);
        long h2 = hash.roll(data[0], data[1]);
        long h3 = hash.roll(data[1], data[2]);
        long h4 = hash.roll(data[2], data[3]);

        assertEquals(h2, h3);
        assertNotEquals(h3, h4);
        assertNotEquals(h3, h1);
        */
    }
}
