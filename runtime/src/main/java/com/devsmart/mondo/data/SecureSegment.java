package com.devsmart.mondo.data;


import com.devsmart.mondo.kademlia.ID;
import com.google.common.hash.HashCode;

public class SecureSegment extends Segment {

    public final HashCode secureHash;
    private transient ID mCachedID;

    public SecureSegment(long offset, long length, HashCode secureHash) {
        super(offset, length);
        this.secureHash = secureHash;
    }

    public ID getID() {
        if(mCachedID == null) {
            mCachedID = new ID(secureHash.asBytes(), 0);
        }
        return mCachedID;
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
