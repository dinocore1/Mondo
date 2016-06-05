package com.devsmart.mondo;


import com.devsmart.mondo.MondoNode;
import com.devsmart.mondo.kademlia.Peer;

public class CreateConnectionTask implements Runnable {

    private final MondoNode mNode;
    private final Peer mTargetPeer;

    public CreateConnectionTask(MondoNode node, Peer targetPeer) {
        mNode = node;
        mTargetPeer = targetPeer;
    }

    @Override
    public void run() {





    }
}
