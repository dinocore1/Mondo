package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.ID;
import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.util.Random;
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
    private DatagramSocket mDatagramSocket;
    private Thread mSocketReader;
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


    private static class ConfigFile {
        String id;
        String localAddress;
        String[] bootstrap;
    }

    public MondoNode(File configDir) {
        mRootDir = configDir;
    }

    private void start() throws Exception {
        initLocalConfig();
        logger.info("Local id: {}", mLocalId.toString(BaseEncoding.base64Url()));
        logger.info("listening on: {}", mDatagramSocket.getLocalSocketAddress());

        mSocketReader = new Thread(mReadSocketTask,
                String.format("%s %s Socket Reader",
                        mLocalId.toString().substring(0, 6),
                        mDatagramSocket.getLocalSocketAddress()));

    }

    private void stop() {
        mDatagramSocket.close();
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
                Message msg = mMessagePool.borrowObject();
                mDatagramSocket.receive(msg.mPacket);
                msg.parseData();


            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
    };

    public static void main(String[] args) {
        String userHome = System.getProperty("user.home");
        File rootDir = new File(userHome, ".mondo");
        localInstance = new MondoNode(rootDir);
        try {
            localInstance.start();
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}
