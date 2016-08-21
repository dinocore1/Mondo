package com.devsmart.mondo.data;


import com.google.common.collect.Iterables;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class SegmentListTest {


    @Test
    public void testMerge() {
        SegmentList segments = new SegmentList();
        Set<Segment> set;

        segments.merge(new Segment(0, 5));
        assertEquals(1, segments.getSegments().size());

        segments.merge(new Segment(5, 5));
        set = segments.getSegments();
        assertEquals(1, set.size());
        assertEquals(Iterables.get(set, 0), new Segment(0, 10));

        segments.merge(new Segment(15, 5));
        set = segments.getSegments();
        assertEquals(2, set.size());

        segments.merge(new Segment(10, 5));
        set = segments.getSegments();
        assertEquals(1, set.size());
        assertEquals(Iterables.get(set, 0), new Segment(0, 20));


    }
}
