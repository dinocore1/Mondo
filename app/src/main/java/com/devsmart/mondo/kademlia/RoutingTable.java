package com.devsmart.mondo.kademlia;


import com.google.common.collect.ComparisonChain;

import java.net.InetSocketAddress;
import java.util.*;

public class RoutingTable {

    private final ID mLocalNode;
    ArrayList<Peer>[] mPeers;

    @SuppressWarnings("unchecked")
    public RoutingTable(ID localId) {
        mLocalNode = localId;
        mPeers = new ArrayList[ID.NUM_BYTES*8];
        for(int i=0;i<ID.NUM_BYTES*8;i++) {
            mPeers[i] = new ArrayList<Peer>(8);
        }
    }

    public ArrayList<Peer> getBucket(ID id) {
        int numBitsInCommon = id.getNumSharedPrefixBits(mLocalNode);
        ArrayList<Peer> bucket = mPeers[numBitsInCommon];
        return bucket;
    }

    public Collection<Peer> getRoutingPeers(ID target) {
        ArrayList<Peer> retval = new ArrayList<Peer>();
        getAllPeers(retval);

        final DistanceComparator distanceComparator = new DistanceComparator(target);
        Comparator<Peer> comparator = new Comparator<Peer>() {
            @Override
            public int compare(Peer a, Peer b) {
                final Peer.Status statusA = a.getStatus();
                final Peer.Status statusB = b.getStatus();

                return ComparisonChain.start()
                        .compareTrueFirst(statusA == Peer.Status.Alive, statusB == Peer.Status.Alive)
                        .compare(a, b, distanceComparator)
                        .result();
            }
        };
        Collections.sort(retval, comparator);
        return retval;
    }

    public Peer getPeer(ID id, InetSocketAddress socketAddress) {
        ArrayList<Peer> bucket = getBucket(id);
        Peer retval = new Peer(id, socketAddress);

        synchronized (bucket) {
            for (Peer p : bucket) {
                if (p.equals(retval)) {
                    retval = p;
                    break;
                }
            }
        }

        return retval;
    }

    public void addPeer(Peer peer) {
        if(peer.id.equals(mLocalNode)) {
            return;
        }

        ArrayList<Peer> bucket = getBucket(peer.id);
        synchronized (bucket) {
            bucket.add(peer);
        }
    }

    public void getAllPeers(Collection<Peer> peerList) {
        for(int i=0;i<ID.NUM_BYTES*8;i++){
            peerList.addAll(mPeers[i]);
        }
    }
}
