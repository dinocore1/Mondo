package com.devsmart.mondo;


import java.net.DatagramPacket;

public class Message {


    byte[] mRawData = new byte[64*1024];
    DatagramPacket mPacket = new DatagramPacket(mRawData, mRawData.length);

    public boolean parseData() {

    }

    public int version() {

    }
}
