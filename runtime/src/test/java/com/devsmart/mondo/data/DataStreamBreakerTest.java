package com.devsmart.mondo.data;


import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.junit.Test;

import java.io.*;
import java.util.IntSummaryStatistics;
import java.util.Random;
import java.util.Set;

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
    public void testStats() throws Exception {

        RandomInputStream stream = new RandomInputStream(new Random(1), 100 * 1024*1024);
        DataStreamBreaker dataBreaker = new DataStreamBreaker(Hashing.md5(), 16);


        SummaryStatistics stats = new SummaryStatistics();

        for(SecureSegment s : dataBreaker.getSegments(stream)){
            System.out.println("l: " + s.length);
            stats.addValue(s.length);
        }

        System.out.println(String.format("num segments: %d avg len: %f stddiv: %f min: %g max: %g",
                stats.getN(), stats.getMean(), stats.getStandardDeviation(), stats.getMin(), stats.getMax()));



    }

    @Test
    public void testFindCommonSegments() throws Exception {

        //DataStreamBreaker dataBreaker = new DataStreamBreaker(Hashing.md5(), 16, 30*1024, 90*1024);
        DataStreamBreaker dataBreaker = new DataStreamBreaker(Hashing.md5(), 14);

        Set<SecureSegment> d1Segments;
        Set<SecureSegment> d2Segments;
        {
            FileInputStream d1 = new FileInputStream(new File("data/121.apk"));
            d1Segments = Sets.newHashSet(dataBreaker.getSegments(d1));
            d1.close();

            SummaryStatistics stats = new SummaryStatistics();
            for(SecureSegment s : d1Segments) {
                stats.addValue(s.length);
            }
            System.out.println(String.format("num segments: %d avg len: %f stddiv: %f min: %10d max: %10d",
                    stats.getN(), stats.getMean(), stats.getStandardDeviation(), (long)stats.getMin(), (long)stats.getMax()));
        }
        {
            FileInputStream d1 = new FileInputStream(new File("data/123.apk"));
            d2Segments = Sets.newHashSet(dataBreaker.getSegments(d1));
            d1.close();

            SummaryStatistics stats = new SummaryStatistics();
            for(SecureSegment s : d2Segments) {
                stats.addValue(s.length);
            }
            System.out.println(String.format("num segments: %d avg len: %f stddiv: %f min: %10d max: %10d",
                    stats.getN(), stats.getMean(), stats.getStandardDeviation(), (long)stats.getMin(), (long)stats.getMax()));
        }

        Sets.SetView<SecureSegment> commonSegments = Sets.intersection(d1Segments, d2Segments);
        long totalCommonBytes = 0;
        for(SecureSegment s : commonSegments) {
            totalCommonBytes += s.length;
        }
        System.out.println(String.format("common segments found: %d common bytes: %d",
                commonSegments.size(), totalCommonBytes));


    }


    @Test
    public void testDataStreamSegments() throws Exception {

        final int numBits = 11;
        byte[] data = new byte[4*1024];

        Random r = new Random(1);
        r.nextBytes(data);

        DataStreamBreaker dataBreaker = new DataStreamBreaker(Hashing.md5(), numBits);
        Iterable<SecureSegment> segments1 = dataBreaker.getSegments(new ByteArrayInputStream(data));


        data[49] = 24;
        dataBreaker = new DataStreamBreaker(Hashing.md5(), numBits);
        Iterable<SecureSegment> segments2 = dataBreaker.getSegments(new ByteArrayInputStream(data));

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
        Iterable<SecureSegment> segments1 = dataBreaker.getSegments(new ByteArrayInputStream(data));


        dataBreaker = new DataStreamBreaker(Hashing.md5(), numBits);
        Iterable<SecureSegment> segments2 = dataBreaker.getSegments(new SequenceInputStream(
                new ByteArrayInputStream(new byte[]{0x43}),
                new ByteArrayInputStream(data)));

        System.out.println(String.format("%s\n%s",
                Iterables.toString(segments1),
                Iterables.toString(segments2)));


    }
}
