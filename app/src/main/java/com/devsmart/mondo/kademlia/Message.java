package com.devsmart.mondo.kademlia;


import com.devsmart.ubjson.UBReader;
import com.devsmart.ubjson.UBValueFactory;
import com.google.common.base.Throwables;

import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class Message {

    public static final int PING = 0;
    public static final int FINDPEERS = 1;

    private static final int FLAG_RESPONSE = 0x10;
    /*

        Version: 2bits
        X: Reserved: 2bits
        R: (Response): 1bit
        PT (Payload Type): 3bits

         0 1 2 3 4 5 6 7 8
        +-+-+-+-+-+-+-+-+-
        |Ver|   |R| PT  |


        # SocketAddress #
          0   1   2   3   4   5
        +------------------------+
        | IPv4 Addr.    | Port   |

        ID: 20-byte value
        SocketAddress: 4-byte address, 2 byte port

        ## Payload Types ##

        ### Ping ###
        PT: 0

        Request Payload:
        ID: Local Peer's ID

        Response Payload:
        ID: Remote Peer's ID
        SocketAddress: the socket address that the local peer is running

        ### FindPeers ###
        PT: 1

        Request Payload:
        ID

        Response Payload:

        {ID, SocketAddress}[]

        ### Connect ###
        PT: 2

        Request Payload:
        ID, SocketAddress

        */
    byte[] mRawData = new byte[64*1024];
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

    public boolean isResponse() {
        return (FLAG_RESPONSE & mRawData[0]) > 0;
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) mPacket.getSocketAddress();
    }

    public void prepareReceive() {
        mPacket.setData(mRawData);
    }

    public static class PingMessage {

        public static ID getId(Message msg) {
            return new ID(msg.mRawData, 1);
        }

        public static InetSocketAddress getAddress(Message msg) {
            try {
                final int offset = 1 + ID.NUM_BYTES;
                byte[] addressData = new byte[4];
                System.arraycopy(msg.mRawData, offset, addressData, 0, 4);
                InetAddress address = InetAddress.getByAddress(addressData);

                int port = msg.mRawData[offset + 4] & 0xFF;
                port |= ((msg.mRawData[offset + 5] << 8) & 0xFF00);

                return new InetSocketAddress(address, port);
            } catch (Exception e) {
                Throwables.propagate(e);
                return null;
            }
        }

        public static void format(Message msg, ID id) {
            msg.mRawData[0] = PING;
            id.write(msg.mRawData, 1);
            msg.mPacket.setData(msg.mRawData, 0, 1 + ID.NUM_BYTES);
        }
    }

    public static class PongMessage {

        public static void formatPong(Message msg, ID id, InetSocketAddress socketAddress) {
            msg.mRawData[0] = PING | FLAG_RESPONSE;

            id.write(msg.mRawData, 1);

            final int offset = 1 + ID.NUM_BYTES;
            byte[] addressData = socketAddress.getAddress().getAddress();
            System.arraycopy(addressData, 0, msg.mRawData, offset, 4);

            final int port = socketAddress.getPort();
            msg.mRawData[offset + 4] = (byte) (0xFF & port);
            msg.mRawData[offset + 5] = (byte) (0xFF & (port >>> 8));

            msg.mPacket.setData(msg.mRawData, 0, 1 + ID.NUM_BYTES + 6);
        }
    }

    public static class FindPeersMessage {

        public static void formatRequest(Message msg, ID targetId) {
            msg.mRawData[0] = FINDPEERS;

            targetId.write(msg.mRawData, 1);

            msg.mPacket.setData(msg.mRawData, 0, 1 + ID.NUM_BYTES);
        }

        public static void formatResponse(Message msg) {
            msg.mRawData[0] = FINDPEERS | FLAG_RESPONSE;
        }

        public static ID getTargetId(Message msg) {
            return new ID(msg.mRawData, 1);
        }
    }
}
