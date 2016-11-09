package com.devsmart.mondo.data;


public class Block implements Comparable<Block> {

    public final long offset;
    public final int len;
    public final long secondaryOffset;


    public Block(long offset, int len, long secondaryOffset) {
        this.offset = offset;
        this.len = len;
        this.secondaryOffset = secondaryOffset;
    }

    public Block(long offset, int len) {
        this(offset, len, 0);
    }

    public long end() {
        return offset + len;
    }

    public boolean containsOffset(long offset) {
        return this.offset <= offset && offset < this.offset + len;
    }


    public boolean intersects(Block b) {
        if(offset == b.offset) {
            return true;
        } else if(offset < b.offset) {
            return end() > b.offset;
        } else {
            return b.end() > offset;
        }
    }

    public Block union(Segment s) {
        final long start = Math.min(offset, s.offset);
        final long end = Math.max(end(), s.end());
        return new Block(start, (int) (end-start));
    }


    @Override
    public int compareTo(Block o) {
        return Long.compare(offset, o.offset);
    }

    @Override
    public int hashCode() {
        return (int) (offset ^ len);
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Block)) {
            return false;
        }
        Block other = (Block) obj;
        return offset == other.offset && len == other.len;
    }

    @Override
    public String toString() {
        return String.format("[%d-%d]", offset, end());
    }
}
