package com.devsmart.mondo;

import com.devsmart.mondo.storage.FilesystemStorage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static File mRootDir;
    private static ConfigFile mConfigFile;
    private static FilesystemStorage mFilesystemStorage;

    public static Gson mGson = new GsonBuilder().create();


    private static final class ConfigFile {
        String username;
        String password;

    }

    public static void main(String[] args) {

        try {
            String homedirPath = System.getProperty("user.home");
            mRootDir = new File(new File(homedirPath), ".mondofs");

            File configFile = new File(mRootDir, "mondo.cfg");
            JsonReader reader = new JsonReader(new FileReader(configFile));
            mConfigFile = mGson.fromJson(reader, ConfigFile.class);

            mFilesystemStorage = new FilesystemStorage(new File(mRootDir, "data"));




        } catch (Exception e) {
            LOGGER.error("", e);
            System.exit(-1);
        }
    }



}
