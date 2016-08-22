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

    public SegmentList() {

    }

    public SegmentList(Iterable<? extends Segment> segments) {
        for(Segment s : segments) {
            mSegments.add(s);
        }
    }

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

    public Set<Segment> getSegments() {
        return Collections.unmodifiableSet(mSegments);
    }

    public Segment getContainer(Segment s) {
        for(Segment existingSegment : mSegments) {
            if(existingSegment.intersects(s)) {
                long end = Math.min(existingSegment.end(), s.end());
                return new Segment(s.offset, end - s.offset);

            }
        }
        return null;
    }

    public long end() {
        if(mSegments.isEmpty()) {
            return 0;
        } else {
            return mSegments.last().end();
        }
    }
}
