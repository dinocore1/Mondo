package com.devsmart.mondo.data;


import com.google.common.hash.HashCode;

public class SecureSegment extends Segment {

    public final HashCode secureHash;

    public SecureSegment(long offset, long length, HashCode secureHash) {
        super(offset, length);
        this.secureHash = secureHash;
    }


    @Override
    public int hashCode() {
        return secureHash.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) {
            return false;
        }
        if(obj instanceof SecureSegment) {
            return secureHash.equals(((SecureSegment) obj).secureHash);
        } else if(obj instanceof HashCode) {
            return secureHash.equals(obj);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("%d:%d %s", offset, length, secureHash);
    }
}
