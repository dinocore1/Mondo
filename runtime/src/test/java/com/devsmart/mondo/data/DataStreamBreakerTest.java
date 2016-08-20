package com.devsmart.mondo.data;


import com.google.common.collect.Iterables;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Random;

public class DataStreamBreakerTest {

    private static class RandomInputStream extends InputStream {

        private final Random mRandom;
        private final long mLength;
        private int mOffset;

        public RandomInputStream(Random r, long length) {
            mRandom = r;
            mLength = length;

        }

        @Override
        public int read() throws IOException {
            if(mOffset++ == mLength){
                return -1;
            } else {
                return mRandom.nextInt(256);
            }
        }
    }


    @Test
    public void testDataStreamSegments() throws Exception {

        final int numBits = 11;
        byte[] data = new byte[4*1024];

        Random r = new Random(1);
        r.nextBytes(data);

        DataStreamBreaker dataBreaker = new DataStreamBreaker(Hashing.md5(), numBits);
        Iterable<Segment> segments1 = dataBreaker.getSegments(new ByteArrayInputStream(data));


        data[49] = 24;
        dataBreaker = new DataStreamBreaker(Hashing.md5(), numBits);
        Iterable<Segment> segments2 = dataBreaker.getSegments(new ByteArrayInputStream(data));

        System.out.println(String.format("%s\n%s",
                Iterables.toString(segments1),
                Iterables.toString(segments2)));


    }

    @Test
    public void testInsertPrefix() throws Exception {

        final int numBits = 11;
        byte[] data = new byte[4*1024];

        Random r = new Random(1);
        r.nextBytes(data);

        DataStreamBreaker dataBreaker = new DataStreamBreaker(Hashing.md5(), numBits);
        Iterable<Segment> segments1 = dataBreaker.getSegments(new ByteArrayInputStream(data));


        dataBreaker = new DataStreamBreaker(Hashing.md5(), numBits);
        Iterable<Segment> segments2 = dataBreaker.getSegments(new SequenceInputStream(
                new ByteArrayInputStream(new byte[]{0x43}),
                new ByteArrayInputStream(data)));

        System.out.println(String.format("%s\n%s",
                Iterables.toString(segments1),
                Iterables.toString(segments2)));


    }
}
