package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.Peer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TrimBucketTaskTest {


    @Test
    public void testBucketSort() {

        ArrayList<Peer> bucket = new ArrayList<Peer>();

        Peer alivePeer2 = mock(Peer.class);
        when(alivePeer2.getStatus()).thenReturn(Peer.Status.Alive);
        when(alivePeer2.getFirstSeen()).thenReturn(20l * 1000l);
        bucket.add(alivePeer2);

        Peer alivePeer1 = mock(Peer.class);
        when(alivePeer1.getStatus()).thenReturn(Peer.Status.Alive);
        when(alivePeer1.getFirstSeen()).thenReturn(1l * 1000l);
        bucket.add(alivePeer1);


        Peer deadPeer = mock(Peer.class);
        when(deadPeer.getStatus()).thenReturn(Peer.Status.Dead);
        when(deadPeer.getFirstSeen()).thenReturn(30l * 1000l);
        bucket.add(deadPeer);


        Collections.sort(bucket, TrimBucketTask.COMPARATOR);

        assertTrue(bucket.get(0) == deadPeer);
        assertTrue(bucket.get(2) == alivePeer2);

    }
}
