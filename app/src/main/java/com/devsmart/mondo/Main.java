package com.devsmart.mondo;

import co.paralleluniverse.javafs.JavaFS;
import com.devsmart.mondo.storage.FilesystemStorage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static File mRootDir;
    private static ConfigFile mConfigFile;
    private static FilesystemStorage mFilesystemStorage;
    private static boolean mRunning;

    public static Gson mGson = new GsonBuilder().create();
    private static Path mMountPoint;
    private static MondoFileStore mFileStore;


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

            File dataRoot = new File(mRootDir, "data");
            //mFilesystemStorage = new FilesystemStorage(dataRoot);

            DB db = DBMaker.fileDB(new File(mRootDir, "db"))
                    .transactionEnable()
                    .make();

            mFileStore = new MondoFileStore(db, dataRoot);
            MondoFilesystemProvider provider = new MondoFilesystemProvider(mFileStore);
            MondoFilesystem fs = new MondoFilesystem(provider, mFileStore);

            final File mountDir = new File(mConfigFile.mount);
            LOGGER.info("mount on: {}", mountDir.getAbsolutePath());
            mMountPoint = mountDir.toPath();

            JavaFS.mount(fs, mMountPoint, false, false);

            mRunning = true;

            Runtime.getRuntime().addShutdownHook(new ShutdownThread());

            while(mRunning) {
                Thread.sleep(500);
            }


        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            try {
                JavaFS.unmount(mMountPoint);
                mFileStore.close();
            } catch (IOException e) {
                LOGGER.error("", e);
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
