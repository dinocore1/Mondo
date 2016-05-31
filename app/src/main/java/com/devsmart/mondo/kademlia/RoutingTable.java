package com.devsmart.mondo.kademlia;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;

public class RoutingTable {

    private final ID mLocalNode;
    private final DistanceComparator mDistanceComparator;
    ArrayList<Peer>[] mPeers;

    private class DistanceComparator implements Comparator<Peer> {
        @Override
        public int compare(Peer a, Peer b) {
            return ID.compareDistance(a.id, b.id, mLocalNode);
        }
    }

    @SuppressWarnings("unchecked")
    public RoutingTable(ID localId) {
        mLocalNode = localId;
        mPeers = new ArrayList[ID.NUM_BYTES*8];
        for(int i=0;i<ID.NUM_BYTES*8;i++) {
            mPeers[i] = new ArrayList<Peer>(8);
        }
        mDistanceComparator = new DistanceComparator();
    }

    public ArrayList<Peer> getBucket(ID id) {
        int numBitsInCommon = id.getNumSharedPrefixBits(mLocalNode);
        ArrayList<Peer> bucket = mPeers[numBitsInCommon];
        return bucket;
    }

    public Peer getPeer(ID id) {
        ArrayList<Peer> bucket = getBucket(id);

        synchronized (bucket) {
            for (Peer p : bucket) {
                if (p.id.equals(id)) {
                    return p;
                }
            }
        }

        return null;
    }

    public Peer addPeer(ID id) {
        ArrayList<Peer> bucket = getBucket(id);

        synchronized (bucket) {
            for (Peer p : bucket) {
                if (p.id.equals(id)) {
                    return p;
                }
            }

            Peer p = new Peer(id);
            bucket.add(p);
            return p;
        }
    }

    public void getAllPeers(Collection<Peer> peerList) {
        for(int i=0;i<ID.NUM_BYTES*8;i++){
            peerList.addAll(mPeers[i]);
        }
    }
}
