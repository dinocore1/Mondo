package com.devsmart.mondo.kademlia;


import com.google.common.collect.ComparisonChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class TrimBucketTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TrimBucketTask.class);

    public static final int NUM_PEERS_PER_BUCKET = 8;
    public static final int NUM_ALIVE_PEERS_PER_BUCKET = 3;


    static final Comparator<Peer> COMPARATOR = new Comparator<Peer>() {
        @Override
        public int compare(Peer a, Peer b) {
            final Peer.Status statusA = a.getStatus();
            final Peer.Status statusB = b.getStatus();
            int result = ComparisonChain.start()
                    .compareTrueFirst(statusA == Peer.Status.Dead, statusB == Peer.Status.Dead)
                    .compareTrueFirst(statusA == Peer.Status.Dying, statusB == Peer.Status.Dying)
                    .compare(a.getAge(), b.getAge())
                    .result();

            return result;
        }
    };


    private final RoutingTable mRoutingTable;

    public TrimBucketTask(RoutingTable table) {
        mRoutingTable = table;
    }

    private void killPeer(Peer peer) {
        logger.debug("pruning peer: {}", peer);
        peer.stopKeepAlive();
    }

    @Override
    public void run() {
        logger.debug("pruning peers...");
        int numPrunedPeers = 0;
        for(ArrayList<Peer> bucket : mRoutingTable.mPeers) {
            synchronized (bucket) {
                Collections.sort(bucket, COMPARATOR);

                Iterator<Peer> it = bucket.iterator();
                while(it.hasNext()) {
                    final Peer peer = it.next();
                    if(bucket.size() > NUM_PEERS_PER_BUCKET
                            || peer.getStatus() == Peer.Status.Dead) {
                        killPeer(peer);
                        it.remove();
                        numPrunedPeers++;
                    }
                }

            }
        }
        logger.debug("pruned {} peers", numPrunedPeers);
    }
}
