package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.*;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
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
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MondoNode {

    private static Logger logger = LoggerFactory.getLogger(MondoNode.class);
    private static MondoNode localInstance;

    private static final Pattern INETADDRESS_REGEX = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");

    private final File mRootDir;
    private File mConfigFile;
    private ConfigFile mConfig;
    private ID mLocalId;
    private RoutingTable mRoutingTable;
    private DatagramSocket mDatagramSocket;
    private Thread mSocketReader;
    private ScheduledExecutorService mTaskExecutors = Executors.newScheduledThreadPool(3, new ThreadFactory() {

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
    private ObjectPool<Message> mMessagePool = new GenericObjectPool<Message>(new BasePooledObjectFactory<Message>() {
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
    }

    public void stop() {
        mFindPeersTask.cancel(false);
        mTrimBucketTask.cancel(false);
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

        ID remoteId = Message.PingMessage.getId(msg);
        Peer remotePeer = mRoutingTable.getPeer(remoteId, msg.getRemoteSocketAddress());
        remotePeer.markSeen();

        if (msg.isResponse()) {
            logger.debug("PONG from {}", remotePeer);

        } else {
            logger.debug("PING from {}", remotePeer);
            //send pong
            sendPong(msg.getRemoteSocketAddress());
        }

        if(isInteresting(remotePeer)){
            mRoutingTable.addPeer(remotePeer);
            remotePeer.startKeepAlive(mTaskExecutors, mLocalId, mDatagramSocket);
        }

    }

    private void handleFindPeers(Message msg) {

        ID remoteId = Message.PingMessage.getId(msg);
        Peer remotePeer = mRoutingTable.getPeer(remoteId, msg.getRemoteSocketAddress());
        remotePeer.markSeen();

        logger.debug("FINDPEERS from {}", remotePeer);

        if(isInteresting(remotePeer)){
            mRoutingTable.addPeer(remotePeer);
            remotePeer.startKeepAlive(mTaskExecutors, mLocalId, mDatagramSocket);
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
        ArrayList<Peer> bucket = mRoutingTable.getBucket(peer.id);
        if(bucket.contains(peer)) {
            return false;
        }


        int alivePeers = 0;
        synchronized (bucket) {
            for(Peer p : bucket) {

                alivePeers += p.getStatus() == Peer.Status.Alive ? 1 : 0;
            }
        }

        return alivePeers < TrimBucketTask.NUM_ALIVE_PEERS_PER_BUCKET;
    }

    public void sendPong(InetSocketAddress socketAddress) {
        try {
            Message pongMsg = mMessagePool.borrowObject();
            try {
                Message.PongMessage.formatPong(pongMsg,
                        mLocalId,
                        (InetSocketAddress) mDatagramSocket.getLocalSocketAddress());

                pongMsg.mPacket.setSocketAddress(socketAddress);
                mDatagramSocket.send(pongMsg.mPacket);

            } finally {
                mMessagePool.returnObject(pongMsg);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public void sendFindPeers(InetSocketAddress inetSocketAddress, ID targetId) {
        try {
            Message msg = mMessagePool.borrowObject();
            try {

                Message.FindPeersMessage.format(msg, targetId);
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
