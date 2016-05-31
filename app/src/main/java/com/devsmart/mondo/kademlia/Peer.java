package com.devsmart.mondo.kademlia;


import com.google.common.io.BaseEncoding;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private Future<?> mKeepAliveTask;


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

    public void startKeepAlive(ScheduledExecutorService executorService, ID localId, DatagramSocket socket) {
        if(mKeepAliveTask != null) {
            KeepAliveTask task = new KeepAliveTask(this, localId, socket);
            mKeepAliveTask = executorService.scheduleWithFixedDelay(task, 1, 1, TimeUnit.SECONDS);
        }
    }

    public void stopKeepAlive() {
        if(mKeepAliveTask != null) {
            mKeepAliveTask.cancel(false);
            mKeepAliveTask = null;
        }
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
