package com.devsmart.mondo;


import com.devsmart.mondo.kademlia.ID;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Mondo {

    private static Logger logger = LoggerFactory.getLogger(Mondo.class);
    private static Mondo localInstance;


    private final File mRootDir;
    private File mConfigFile;
    private ConfigFile mConfig;
    private ID mLocalId;

    private static class ConfigFile {
        String id;
        String[] bootstrap;
    }

    public Mondo(File configDir) {
        mRootDir = configDir;
    }



    private void start() throws Exception {
        initLocalConfig();
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
        logger.info("Local id: {}", mLocalId.toString(BaseEncoding.base64Url()));

    }

    public static void main(String[] args) {
        String userHome = System.getProperty("user.home");
        File rootDir = new File(userHome, ".mondo");
        localInstance = new Mondo(rootDir);
        try {
            localInstance.start();
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}
