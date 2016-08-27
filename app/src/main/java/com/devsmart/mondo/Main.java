package com.devsmart.mondo;

import com.devsmart.mondo.storage.FilesystemStorage;
import com.devsmart.mondo.storage.VirtualFilesystem;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.apache.commons.lang3.SystemUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static File mRootDir;
    private static ConfigFile mConfigFile;
    private static FilesystemStorage mFilesystemStorage;
    private static VirtualFilesystem mVirtualFilesystem;
    private static UserspaceFilesystem mUserspaceFS;
    private static boolean mRunning;

    public static Gson mGson = new GsonBuilder().create();


    private static final class ConfigFile {
        String username;
        String password;
        String mount;

    }

    public static void main(String[] args) {

        try {
            String homedirPath = System.getProperty("user.home");
            mRootDir = new File(new File(homedirPath), ".mondofs");

            File configFile = new File(mRootDir, "mondo.cfg");
            JsonReader reader = new JsonReader(new FileReader(configFile));
            mConfigFile = mGson.fromJson(reader, ConfigFile.class);

            mFilesystemStorage = new FilesystemStorage(new File(mRootDir, "data"));

            DB db = DBMaker.fileDB(new File(mRootDir, "db"))
                    .transactionEnable()
                    .make();

            Class<? extends UserspaceFilesystem> userspaceFPClass = null;
            if(SystemUtils.IS_OS_UNIX) {
                mVirtualFilesystem = new VirtualFilesystem(db, '/');
                userspaceFPClass = (Class<? extends UserspaceFilesystem>) Main.class.getClassLoader().loadClass("com.devsmart.mondo.data.FUSEUserspaceFilesystem");
            }

            mUserspaceFS = userspaceFPClass.newInstance();
            mUserspaceFS.init(mVirtualFilesystem, mFilesystemStorage);
            final File mountDir = new File(mConfigFile.mount);
            LOGGER.info("mount on: {}", mountDir.getAbsolutePath());
            mUserspaceFS.mount(mountDir);

            mRunning = true;

            Runtime.getRuntime().addShutdownHook(new ShutdownThread());

            while(mRunning) {
                Thread.sleep(500);
            }


        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if(mUserspaceFS != null) {
                try {
                    mUserspaceFS.unmount();
                }catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
            if(mVirtualFilesystem != null) {
                try {
                    mVirtualFilesystem.close();
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
        }
    }

    private static class ShutdownThread extends Thread {

        @Override
        public void run() {
            LOGGER.info("got shutdown signal");
            mRunning = false;
        }
    }



}
