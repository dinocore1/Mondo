package com.devsmart.mondo.kademlia;


import com.google.common.io.BaseEncoding;

import java.net.InetSocketAddress;

public class Peer {

    public enum Status {
        Alive,
        Dieing,
        Dead
    }

    public final ID id;
    private InetSocketAddress mSocketAddress;
    private long mFirstSeen;
    private long mLastSeen;

    public Peer(ID id) {
        this.id = id;
        mFirstSeen = System.nanoTime();
    }

    public Peer(ID id, InetSocketAddress socketAddress) {
        this.id = id;
        this.mSocketAddress = socketAddress;
        mFirstSeen = System.nanoTime();
    }

    public void setSocketAddress(InetSocketAddress socketAddress) {
        mSocketAddress = socketAddress;
    }

    public InetSocketAddress getInetSocketAddress() {
        return mSocketAddress;
    }

    public void markSeen() {
        mLastSeen = System.nanoTime();
    }

    public long getLastSeenMillisec() {
        long retval = System.nanoTime() - mLastSeen;
        return retval / 1000000;
    }

    public long getAge() {
        long retval = System.nanoTime() - mFirstSeen;
        return retval / 1000000;
    }

    public Status getStatus() {
        final long lastSeen = getLastSeenMillisec();
        if(lastSeen < 5000) {
            return Status.Alive;
        } else if(lastSeen < 10000) {
            return Status.Dieing;
        } else {
            return Status.Dead;
        }
    }

    @Override
    public String toString() {
        return String.format("%s:%s",
                id.toString(BaseEncoding.base64Url()).substring(0, 6),
                mSocketAddress
        );
    }
}
