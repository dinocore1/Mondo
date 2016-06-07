package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
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
                        int count = 0;
                        for(Peer viaPeer : routingCanidates) {
                            if(!viaPeer.equals(peer)) {
                                logger.debug("trying to connect to {} via {}", peer.id, viaPeer);
                                List<InetSocketAddress> localAddresses = mNode.mLocalSocketAddressConsensus.getMostLikely();
                                addLocalAddresses(localAddresses);

                                mNode.sendConnect(viaPeer.getInetSocketAddress(), 0, peer.id, mNode.getLocalId(), localAddresses);

                                Thread.sleep(333);

                                peer.startKeepAlive(mNode.mTaskExecutors, mNode.getLocalId(), mNode.mDatagramSocket);

                                if(++count > 3){
                                    break;
                                }
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            logger.error("", e);
        }


    }

    private void addLocalAddresses(List<InetSocketAddress> localAddresses) {
        try {
            for (NetworkInterface iface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if(!iface.isLoopback() && iface.isUp()) {
                    for(InetAddress address : Collections.list(iface.getInetAddresses())){
                        if(address instanceof Inet4Address && !address.isAnyLocalAddress() && !address.isLoopbackAddress()) {
                            localAddresses.add(new InetSocketAddress(address, mNode.mDatagramSocket.getLocalPort()));
                        }
                    }
                }

            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }


}
