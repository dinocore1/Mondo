package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MaintainConnectionsTask implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(MaintainConnectionsTask.class);
    public static final Random mRandom = new Random();

    private final MondoNode mNode;

    public MaintainConnectionsTask(MondoNode node) {
        mNode = node;
    }

    @Override
    public void run() {

        try {
            logger.debug("Maintain peer connections...");

            LinkedList<Peer> allPeers = new LinkedList<Peer>();
            mNode.getRoutingTable().getAllPeers(allPeers);

            Iterator<Peer> it = allPeers.iterator();
            while (it.hasNext()) {
                Peer peer = it.next();

                if (peer.getStatus() == Peer.Status.Unknown && mNode.isInteresting(peer)) {

                    List<Peer> routingCanidates = mNode.getRoutingTable().getRoutingPeers(peer.id);
                    if (!routingCanidates.isEmpty()) {
                        Peer viaPeer = routingCanidates.get(Math.min(routingCanidates.size(), mRandom.nextInt(8)));
                        logger.debug("trying to connect to {} via {}", peer.id, viaPeer);
                        mNode.sendConnect(viaPeer.getInetSocketAddress(), 0, peer.id);

                        Thread.sleep(333);

                        peer.startKeepAlive(mNode.mTaskExecutors, mNode.getLocalId(), mNode.mDatagramSocket);
                    }

                }
            }

        } catch (Exception e) {
            logger.error("", e);
        }


    }


}
