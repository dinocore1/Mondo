package com.devsmart.mondo.kademlia;


import com.devsmart.mondo.MondoNode;
import com.google.common.collect.ComparisonChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

public class FindPeersTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FindPeersTask.class);

    private final MondoNode mNode;
    private final Collection<InetSocketAddress> mBootstrapNodes;

    static final int MIN_NUM_PEERS = 8;

    Comparator<Peer> COMPARATOR = new Comparator<Peer>() {
        @Override
        public int compare(Peer a, Peer b) {
            final Peer.Status statusA = a.getStatus();
            final Peer.Status statusB = b.getStatus();

            final ID localId = mNode.getLocalId();
            final int numCommonA = a.id.getNumSharedPrefixBits(localId);
            final int numCommonB = b.id.getNumSharedPrefixBits(localId);

            int retval = ComparisonChain.start()
                    .compareTrueFirst(statusA == Peer.Status.Alive, statusB == Peer.Status.Alive)
                    .compareTrueFirst(statusA == Peer.Status.Dieing, statusB == Peer.Status.Dieing)
                    .compare(numCommonB, numCommonA)
                    .compare(b.getAge(), a.getAge())
                    .result();

            return retval;
        }
    };

    public FindPeersTask(MondoNode node, Collection<InetSocketAddress> bootstrapNodes) {
        mNode = node;
        mBootstrapNodes = bootstrapNodes;
    }

    @Override
    public void run() {
        logger.debug("Finding new peers...");

        try {
            LinkedList<Peer> allPeers = new LinkedList<Peer>();
            mNode.getRoutingTable().getAllPeers(allPeers);

            if(allPeers.isEmpty()) {
                if(mBootstrapNodes == null || mBootstrapNodes.isEmpty()){
                    logger.warn("no bootstrap nodes available");
                } else {
                    for (InetSocketAddress bootstrapAddress : mBootstrapNodes) {
                        logger.debug("asking {} for new peers", bootstrapAddress);
                        mNode.sendFindPeers(bootstrapAddress, mNode.getLocalId());
                        Thread.sleep(500);
                    }
                }
            } else {
                Collections.sort(allPeers, COMPARATOR);
                Iterator<Peer> it = allPeers.iterator();
                int numRequestsSent = 0;
                while(it.hasNext()) {
                    Peer peer = it.next();
                    logger.debug("asking {} for new peers", peer);
                    mNode.sendFindPeers(peer.getInetSocketAddress(), mNode.getLocalId());
                    Thread.sleep(500);
                    numRequestsSent++;
                    if(numRequestsSent > MIN_NUM_PEERS) {
                        break;
                    }
                }
            }

        } catch (Exception e) {
            logger.error("", e);
        }

    }
}
