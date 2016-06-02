package com.devsmart.mondo.kademlia;


import com.google.common.io.BaseEncoding;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Peer {

    private static final int TIME_DIEING = 15 * 1000;
    private static final int TIME_DEAD = 60 * 1000;

    public enum Status {
        Unknown,
        Alive,
        Dying,
        statusA, Dead
    }

    public final ID id;
    private InetSocketAddress mSocketAddress;
    private long mFirstSeen = -1;
    private long mLastSeen = -1;
    private Future<?> mKeepAliveTask;

    public Peer(ID id, InetSocketAddress socketAddress) {
        this.id = id;
        this.mSocketAddress = socketAddress;
    }

    public InetSocketAddress getInetSocketAddress() {
        return mSocketAddress;
    }

    public void markSeen() {
        mLastSeen = System.nanoTime();
        if(mFirstSeen == -1) {
            mFirstSeen = mLastSeen;
        }
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
        if(lastSeen < TIME_DIEING) {
            return Status.Alive;
        } else if(lastSeen < TIME_DEAD) {
            return Status.Dying;
        } else {
            return Status.Dead;
        }
    }

    public void startKeepAlive(ScheduledExecutorService executorService, ID localId, DatagramSocket socket) {
        if(mKeepAliveTask == null) {
            KeepAliveTask task = new KeepAliveTask(this, localId, socket);
            mKeepAliveTask = executorService.scheduleWithFixedDelay(task, 1, 5, TimeUnit.SECONDS);
        }
    }

    public void stopKeepAlive() {
        if(mKeepAliveTask != null) {
            mKeepAliveTask.cancel(false);
            mKeepAliveTask = null;
        }
    }

    @Override
    public int hashCode() {
        int retval = id.hashCode() ^ mSocketAddress.hashCode();
        return retval;
    }

    @Override
    public boolean equals(Object obj) {
        boolean retval = false;
        if(obj instanceof Peer) {
            Peer o = (Peer)obj;
            retval = id.equals(o.id) && mSocketAddress.equals(o.mSocketAddress);
        }
        return retval;
    }

    @Override
    public String toString() {
        return String.format("%s:%s %s",
                id.toString(BaseEncoding.base64Url()).substring(0, 6),
                mSocketAddress,
                getStatus().name()
        );
    }
}
