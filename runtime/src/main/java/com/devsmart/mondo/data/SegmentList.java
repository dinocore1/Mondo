package com.devsmart.mondo.data;


import java.util.*;

public class SegmentList {

    private static final Comparator<Segment> COMPARATOR = new Comparator<Segment>() {
        @Override
        public int compare(Segment a, Segment b) {
            return Long.compare(a.offset, b.offset);
        }
    };

    private TreeSet<Segment> mSegments = new TreeSet<Segment>(COMPARATOR);

    public synchronized void merge(Segment s) {
        if(mSegments.isEmpty()) {
            mSegments.add(s);
        } else {
            Iterator<Segment> it = mSegments.iterator();
            while(it.hasNext()) {
                Segment existingSegment = it.next();
                if(existingSegment.intersects(s)) {
                    it.remove();
                    merge(existingSegment.union(s));
                    return;
                }
            }
            mSegments.add(s);
        }
    }

    Set<Segment> getSegments() {
        return Collections.unmodifiableSet(mSegments);
    }
}
