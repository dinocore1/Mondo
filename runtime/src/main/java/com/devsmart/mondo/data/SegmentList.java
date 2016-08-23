package com.devsmart.mondo.data;


import java.util.*;

public class SegmentList implements Iterable<Segment> {

    private static final Comparator<Segment> COMPARATOR = new Comparator<Segment>() {
        @Override
        public int compare(Segment a, Segment b) {
            return Long.compare(a.offset, b.offset);
        }
    };

    private LinkedList<Segment> mSegments = new LinkedList<Segment>();

    public SegmentList() {

    }

    public SegmentList(Iterable<? extends Segment> segments) {
        for(Segment s : segments) {
            mSegments.add(s);
        }
    }

    public int size() {
        return mSegments.size();
    }

    @Override
    public Iterator<Segment> iterator() {
        return mSegments.iterator();
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

    public Iterable<Segment> getSegments() {
        return Collections.unmodifiableList(mSegments);
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
            return mSegments.getLast().end();
        }
    }

    public void truncate(long size) {
        ListIterator<Segment> it = mSegments.listIterator();
        while(it.hasNext()) {
            Segment s = it.next();
            if(s.offset >= size) {
                it.remove();
            } else if(s.end() > size) {
                it.set(new Segment(s.offset, s.end() - size));
            }
        }
    }

    public void clear() {
        mSegments.clear();
    }
}
