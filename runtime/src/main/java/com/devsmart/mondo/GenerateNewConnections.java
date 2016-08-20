package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.Peer;
import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.*;

public class GenerateNewConnections implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(GenerateNewConnections.class);
    public static final Random mRandom = new Random();

    private final MondoNode mNode;

    public GenerateNewConnections(MondoNode node) {
        mNode = node;
    }

    @Override
    public void run() {
        try {

            LinkedList<Peer> allPeers = new LinkedList<Peer>();
            mNode.getRoutingTable().getAllPeers(allPeers);
            Collections.shuffle(allPeers, mRandom);
            Optional<Peer> targetPeer = Iterables.tryFind(allPeers, new Predicate<Peer>() {
                @Override
                public boolean apply(Peer input) {
                    return input.getStatus() == Peer.Status.Unknown;
                }
            });
            if(targetPeer.isPresent()) {
                Peer peer = targetPeer.get();
                peer.startKeepAlive(mNode.mTaskExecutors, mNode.getLocalId(), mNode.mDatagramSocket);

                List<Peer> routingCanidates = mNode.getRoutingTable().getRoutingPeers(peer.id);
                if (!routingCanidates.isEmpty()) {
                    int count = 0;
                    for(Peer viaPeer : routingCanidates) {
                        if(!viaPeer.id.equals(peer.id) && viaPeer.getStatus() == Peer.Status.Alive) {
                            logger.debug("trying to connect to {} via {}", peer.id.toString().substring(0, 6), viaPeer);
                            List<InetSocketAddress> localAddresses = mNode.mLocalSocketAddressConsensus.getMostLikely();
                            addLocalAddresses(localAddresses);

                            mNode.sendConnect(viaPeer.getInetSocketAddress(), 0, peer.id, mNode.getLocalId(), localAddresses);

                            Thread.sleep(200);

                            if(++count > 3){
                                break;
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
