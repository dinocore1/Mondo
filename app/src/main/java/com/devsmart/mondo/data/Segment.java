package com.devsmart.mondo.data;


import com.google.common.hash.HashCode;

public class Segment {

    public final long offset;
    public final long length;
    public final HashCode secureHash;

    public Segment(long offset, long length, HashCode secureHash) {
        this.offset = offset;
        this.length = length;
        this.secureHash = secureHash;
    }

    @Override
    public String toString() {
        return String.format("%d:%d %s", offset, length, secureHash);
    }
}
