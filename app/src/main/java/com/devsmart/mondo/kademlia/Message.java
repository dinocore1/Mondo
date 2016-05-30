package com.devsmart.mondo.kademlia;


import java.net.DatagramPacket;
import java.net.InetSocketAddress;

public class Message {

    public static final int PING = 0;
    /*

        Version: 2bits
        X: Reserved: 2bits
        R: (Response): 1bit
        PT (Payload Type): 3bits

                 0
        +---------------+--------+-------+-------+--------+--------+--------+
        | V | X |R|PT


        ID: 20-byte value
        SocketAddress: 4-byte address, 2 byte port

        ## Payload Types ##

        Ping: 0
        ID

        GetPeers: 1
        ID[]

        Connect: 2
        ID, SocketAddress

        */
    private byte[] mRawData = new byte[64*1024];
    public final DatagramPacket mPacket = new DatagramPacket(mRawData, mRawData.length);

    public boolean parseData() {
        return false;
    }

    public int version() {
        return mRawData[0] >>> 6;
    }

    public int getType() {
        return 0x7 & mRawData[0];
    }

    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) mPacket.getSocketAddress();
    }
}
