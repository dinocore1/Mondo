package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.*;
import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.*;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MondoNode {

    private static Logger logger = LoggerFactory.getLogger(MondoNode.class);
    private static MondoNode localInstance;

    private static final Pattern INETADDRESS_REGEX = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");

    private static final int MAX_TTY = 8;

    private final File mRootDir;
    private File mConfigFile;
    private ConfigFile mConfig;
    private ID mLocalId;
    private RoutingTable mRoutingTable;
    protected DatagramSocket mDatagramSocket;
    private Thread mSocketReader;
    ScheduledExecutorService mTaskExecutors = Executors.newScheduledThreadPool(3, new ThreadFactory() {

        int mTreadNum = 0;

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, String.format("%s %s Task %d",
                    mLocalId.toString().substring(0, 6),
                    mDatagramSocket.getLocalSocketAddress(),
                    mTreadNum++));
        }
    });
    private boolean mRunning;
    ObjectPool<Message> mMessagePool = new GenericObjectPool<Message>(new BasePooledObjectFactory<Message>() {
        @Override
        public Message create() throws Exception {
            return new Message();
        }

        @Override
        public PooledObject<Message> wrap(Message obj) {
            return new DefaultPooledObject<Message>(obj);
        }
    });
    private ScheduledFuture<?> mTrimBucketTask;
    private ScheduledFuture<?> mFindPeersTask;
    private ScheduledFuture<?> mMaintainConnections;
    Consensus<InetSocketAddress> mLocalSocketAddressConsensus = new Consensus<InetSocketAddress>(InetSocketAddress.class, 5);


    private static class ConfigFile {
        String id;
        String localAddress;
        String[] bootstrap;
    }

    public MondoNode(File configDir) {
        mRootDir = configDir;
    }

    public ID getLocalId() {
        return mLocalId;
    }

    public RoutingTable getRoutingTable() {
        return mRoutingTable;
    }

    public void start() throws Exception {
        initLocalConfig();
        logger.info("Local id: {}", mLocalId.toString(BaseEncoding.base64Url()));
        logger.info("listening on: {}", mDatagramSocket.getLocalSocketAddress());

        mRunning = true;
        mSocketReader = new Thread(mReadSocketTask,
                String.format("%s %s Socket Reader",
                        mLocalId.toString().substring(0, 6),
                        mDatagramSocket.getLocalSocketAddress()));
        mSocketReader.start();


        mTrimBucketTask = mTaskExecutors.scheduleWithFixedDelay(new TrimBucketTask(mRoutingTable), 60, 30, TimeUnit.SECONDS);

        ArrayList<InetSocketAddress> bootstrapAddresses = new ArrayList<InetSocketAddress>();
        if(mConfig.bootstrap != null) {
            for (String bootstrapStr : mConfig.bootstrap) {
                Matcher m = INETADDRESS_REGEX.matcher(bootstrapStr);
                if (m.find()) {
                    String host = m.group(1);
                    int port = Integer.parseInt(m.group(2));
                    bootstrapAddresses.add(new InetSocketAddress(host, port));
                }
            }
        }

        mFindPeersTask = mTaskExecutors.scheduleWithFixedDelay(new FindPeersTask(this, bootstrapAddresses), 5, 30, TimeUnit.SECONDS);
        mMaintainConnections = mTaskExecutors.scheduleWithFixedDelay(new MaintainConnectionsTask(this), 5, 20, TimeUnit.SECONDS);
    }

    public void stop() {
        mFindPeersTask.cancel(false);
        mTrimBucketTask.cancel(false);
        mMaintainConnections.cancel(false);
        mTaskExecutors.shutdown();
        mDatagramSocket.close();
    }

    public void waitForExit() throws InterruptedException {
        mSocketReader.join();
    }

    private void initLocalConfig() throws IOException {
        logger.info("using local dir: {}", mRootDir.getAbsolutePath());
        mRootDir.mkdirs();

        Gson gson = new GsonBuilder()
                .disableHtmlEscaping()
                .setPrettyPrinting()
                .create();

        mConfigFile = new File(mRootDir, "mondo.cfg");
        if(mConfigFile.exists()) {
            FileReader fileReader = new FileReader(mConfigFile);
            mConfig = gson.fromJson(fileReader, ConfigFile.class);
            fileReader.close();
        } else {
            logger.info("{} config file does not exist, creating new instance", mConfigFile.getAbsolutePath());
            mConfig = new ConfigFile();
            Random r = new Random();
            byte[] iddata = new byte[ID.NUM_BYTES];
            r.nextBytes(iddata);
            ID id = new ID(iddata, 0);
            mConfig.id = id.toString(BaseEncoding.base64Url());

            FileWriter fileWriter = new FileWriter(mConfigFile);
            gson.toJson(mConfig, fileWriter);
            fileWriter.close();
        }

        mLocalId = ID.fromBase64String(mConfig.id);
        mRoutingTable = new RoutingTable(mLocalId);


        if(mConfig.localAddress != null) {
            Matcher m = INETADDRESS_REGEX.matcher(mConfig.localAddress);
            if(m.find()){
                String host = m.group(1);
                int port = Integer.parseInt(m.group(2));
                mDatagramSocket = new DatagramSocket(new InetSocketAddress(host, port));
            } else {
                throw new RuntimeException("unable to parse: " + mConfig.localAddress);
            }
        } else {
            mDatagramSocket = new DatagramSocket();
        }

    }

    private final Runnable mReadSocketTask = new Runnable() {
        @Override
        public void run() {
            try {

                mDatagramSocket.setSoTimeout(500);

                while(mRunning) {
                    Message msg = mMessagePool.borrowObject();
                    try {
                        msg.prepareReceive();
                        mDatagramSocket.receive(msg.mPacket);
                        msg.parseData();

                        switch (msg.getType()) {
                            case Message.PING:
                                handlePing(msg);
                                break;

                            case Message.FINDPEERS:
                                handleFindPeers(msg);
                                break;

                            case Message.CONNECT:
                                handleConnect(msg);
                                break;

                        }
                    } catch (SocketTimeoutException e) {
                    } finally {
                        mMessagePool.returnObject(msg);
                    }
                }


            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
    };

    private void handlePing(Message msg) {

        final ID remoteId = Message.PingMessage.getId(msg);
        final InetSocketAddress remoteAddress = msg.getRemoteSocketAddress();
        Peer remotePeer = mRoutingTable.getPeer(remoteId, remoteAddress);
        remotePeer.markSeen();

        if (msg.isResponse()) {
            logger.debug("PONG from {}", remotePeer);
            mLocalSocketAddressConsensus.vote(Message.PingMessage.getSocketAddress(msg), remoteAddress);

        } else {
            logger.debug("PING from {}", remotePeer);
            //send pong
            try {
                Message pongMsg = mMessagePool.borrowObject();
                try {
                    Message.PingMessage.formatResponse(pongMsg, mLocalId, remoteAddress);
                    pongMsg.mPacket.setSocketAddress(remoteAddress);
                    mDatagramSocket.send(pongMsg.mPacket);

                } finally {
                    mMessagePool.returnObject(pongMsg);
                }
            } catch (Exception e) {
                logger.error("", e);
            }

            if(isInteresting(remotePeer)){
                remotePeer.startKeepAlive(mTaskExecutors, mLocalId, mDatagramSocket);
            }
        }



    }

    private void handleFindPeers(Message msg) {
        try {

            logger.debug("FINDPEERS from {}", msg.mPacket.getSocketAddress());

            if (msg.isResponse()) {
                Collection<Peer> newPeers = Message.FindPeersMessage.getPeers(msg);
                for (Peer p : newPeers) {
                    mRoutingTable.getPeer(p.id, p.getInetSocketAddress());
                }

            } else {
                ID target = Message.FindPeersMessage.getTargetId(msg);
                Collection<Peer> peers = mRoutingTable.getRoutingPeers(target);

                Message resp = mMessagePool.borrowObject();
                try {
                    resp.mPacket.setSocketAddress(msg.getRemoteSocketAddress());
                    Message.FindPeersMessage.formatResponse(resp, peers);
                    mDatagramSocket.send(resp.mPacket);
                } finally {
                    mMessagePool.returnObject(resp);
                }

            }

        } catch (Exception e) {
            logger.error("", e);
        }

    }

    private void handleConnect(Message msg) {

        logger.debug("CONNECT from {}", msg.mPacket.getSocketAddress());

        if (msg.isResponse()) {

        } else {
            final ID target = Message.ConnectMessage.getTargetId(msg);
            final ID fromId = Message.ConnectMessage.getFromId(msg);
            Collection<InetSocketAddress> connectAddresses = Message.ConnectMessage.getSocketAddresses(msg);
            if(mLocalId.equals(target)) {
                for(InetSocketAddress address : connectAddresses) {
                    if(address.getAddress().isAnyLocalAddress() || address.getAddress().isLoopbackAddress()) {
                        continue;
                    }
                    Peer peer = mRoutingTable.getPeer(fromId, address);
                    if(isInteresting(peer)) {
                        peer.startKeepAlive(mTaskExecutors, mLocalId, mDatagramSocket);
                    }
                }

            } else {
                int tty = Message.ConnectMessage.getTTY(msg);
                if(tty < MAX_TTY) {
                    List<Peer> routingCanidates = mRoutingTable.getRoutingPeers(target);
                    int count = 0;
                    for(Peer forwardPeer : routingCanidates) {
                        if(!forwardPeer.getInetSocketAddress().equals(msg.getRemoteSocketAddress())) {
                            sendConnect(forwardPeer.getInetSocketAddress(), tty + 1, target, fromId, connectAddresses);
                            if (++count > 5) {
                                break;
                            }
                        }
                    }

                }
            }
        }

    }

    /**
     * return true if this node decides this peer is "interesting". Where
     * "interesting" is loosely defined as this peer should be keep alive
     * because it is useful for routing.
     * @param peer
     * @return
     */
    public boolean isInteresting(Peer peer) {
        if(peer.id.equals(mLocalId)) {
            return false;
        }

        ArrayList<Peer> bucket = mRoutingTable.getBucket(peer.id);

        int alivePeers = 0;
        synchronized (bucket) {
            for(Peer p : bucket) {
                alivePeers += p.getStatus() == Peer.Status.Alive ? 1 : 0;
            }
        }

        return alivePeers < TrimBucketTask.NUM_ALIVE_PEERS_PER_BUCKET;
    }

    public void sendPing(InetSocketAddress socketAddress) {
        try {
            Message msg = mMessagePool.borrowObject();
            try {
                Message.PingMessage.formatRequest(msg, mLocalId);
                msg.mPacket.setSocketAddress(socketAddress);
                mDatagramSocket.send(msg.mPacket);

            } finally {
                mMessagePool.returnObject(msg);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public void sendFindPeers(InetSocketAddress inetSocketAddress, ID targetId) {
        try {
            Message msg = mMessagePool.borrowObject();
            try {

                Message.FindPeersMessage.formatRequest(msg, targetId);
                msg.mPacket.setSocketAddress(inetSocketAddress);
                mDatagramSocket.send(msg.mPacket);
            } finally {
                mMessagePool.returnObject(msg);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public void sendConnect(InetSocketAddress inetSocketAddress, int tty, ID targetId, ID fromId, Collection<InetSocketAddress> localAddresses) {
        try {
            Message msg = mMessagePool.borrowObject();
            try {
                Message.ConnectMessage.formatRequest(msg, tty, targetId, fromId, localAddresses);
                msg.mPacket.setSocketAddress(inetSocketAddress);
                mDatagramSocket.send(msg.mPacket);
            } finally {
                mMessagePool.returnObject(msg);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public static void main(String[] args) {

        Options options = new Options();

        options.addOption(Option.builder("d")
                .hasArg()
                .argName("data dir")
                .desc("the directory where data is stored for this node")
                .build());

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(options, args);

            File rootDir;
            if(line.hasOption("d")) {
                rootDir = new File(line.getOptionValue("d"));
            } else {
                String userHome = System.getProperty("user.home");
                rootDir = new File(userHome, ".mondo");
            }

            localInstance = new MondoNode(rootDir);

            localInstance.start();
            localInstance.waitForExit();


        } catch (ParseException e) {
            System.err.println("cmd line parse failed: " + e.getMessage());

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("app", options);

            System.exit(-1);
        } catch (Exception e) {
            logger.error("", e);
            System.exit(-1);
        }


    }
}
