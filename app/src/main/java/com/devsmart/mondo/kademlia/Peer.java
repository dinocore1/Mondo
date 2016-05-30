package com.devsmart.mondo.kademlia;


import com.google.common.io.BaseEncoding;

import java.net.InetSocketAddress;

public class Peer {

    private InetSocketAddress mAddress;
    public final ID id;
    private long mLastSeen;

    public Peer(ID id) {
        this.id = id;
    }

    public void markSeen() {
        mLastSeen = System.nanoTime();
    }

    public long getLastSeenMillisec() {
        long retval = System.nanoTime() - mLastSeen;
        return retval / 1000000;
    }

    @Override
    public String toString() {
        return String.format("%s:%s",
                id.toString(BaseEncoding.base64Url()).substring(0, 6),
                mAddress
        );
    }
}
