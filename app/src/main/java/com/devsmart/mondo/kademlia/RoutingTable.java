package com.devsmart.mondo.kademlia;


import java.util.ArrayList;
import java.util.Comparator;

public class RoutingTable {

    private final ID mLocalNode;
    private final DistanceComparator mDistanceComparator;
    ArrayList<Peer> mPeers = new ArrayList<Peer>();

    private class DistanceComparator implements Comparator<Peer> {
        @Override
        public int compare(Peer a, Peer b) {
            return ID.compareDistance(a.id, b.id, mLocalNode);
        }
    }

    public RoutingTable(ID localId) {
        mLocalNode = localId;
        mDistanceComparator = new DistanceComparator();

    }
}
