package com.devsmart.mondo.data;


import com.google.common.base.Preconditions;

public class Segment {

    public final long offset;
    public final long length;

    public Segment(long offset, long length) {
        Preconditions.checkArgument(offset >= 0 && length > 0);
        this.offset = offset;
        this.length = length;
    }

    public boolean intersects(Segment s) {
        if(offset == s.offset) {
            return true;
        } else if(offset < s.offset) {
            return end() >= s.offset;
        } else {
            return s.end() >= offset;
        }
    }

    public boolean contains(Segment s) {
        return offset <= s.offset && end() >= s.end();
    }

    public long end() {
        return offset + length;
    }

    public long unionSize(Segment s) {
        final long start = Math.min(offset, s.offset);
        final long end = Math.max(end(), s.end());
        return end - start;
    }

    public Segment union(Segment s) {
        final long start = Math.min(offset, s.offset);
        final long end = Math.max(end(), s.end());
        return new Segment(start, end-start);
    }

    @Override
    public int hashCode() {
        return (int)(offset ^ length);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj != null && obj.getClass() == getClass()) {
            Segment other = (Segment)obj;
            return offset == other.offset && length == other.length;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("[%d-%d]", offset, end());
    }
}
