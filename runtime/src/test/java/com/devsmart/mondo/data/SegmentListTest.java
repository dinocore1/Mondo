package com.devsmart.mondo.data;


import com.google.common.collect.Iterables;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.*;

public class SegmentListTest {


    @Test
    public void testMerge() {
        SegmentList segments = new SegmentList();

        segments.merge(new Segment(0, 5));
        assertEquals(1, segments.size());

        segments.merge(new Segment(5, 5));
        assertEquals(1, segments.size());
        assertEquals(Iterables.get(segments, 0), new Segment(0, 10));

        segments.merge(new Segment(15, 5));
        assertEquals(2, segments.size());

        segments.merge(new Segment(10, 5));
        assertEquals(1, segments.size());
        assertEquals(Iterables.get(segments, 0), new Segment(0, 20));
    }

    @Test
    public void testTruncate() {
        SegmentList segments = new SegmentList();

        segments.merge(new Segment(0, 20));
        segments.truncate(10);
        assertEquals(1, segments.size());
        assertEquals(Iterables.get(segments, 0), new Segment(0, 10));

        segments.merge(new Segment(15, 5));
        segments.truncate(5);
        assertEquals(1, segments.size());
        assertEquals(Iterables.get(segments, 0), new Segment(0, 5));
    }
}
