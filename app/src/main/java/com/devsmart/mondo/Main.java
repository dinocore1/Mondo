package com.devsmart.mondo;

import co.paralleluniverse.javafs.JavaFS;
import com.devsmart.mondo.storage.MondoFileSystemProvider;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static File mRootDir;
    private static ConfigFile mConfigFile;
    private static boolean mRunning = false;

    public static Gson mGson = new GsonBuilder().create();

    private static final class ConfigFile {
        String username;
        String password;
        String mount;

    }

    private static MondoFileSystemProvider getMondoFileSystemProvider() {
        for (FileSystemProvider fsr : FileSystemProvider.installedProviders()) {
            if (fsr instanceof MondoFileSystemProvider)
                return (MondoFileSystemProvider) fsr;
        }
        throw new AssertionError("Mondo file system not installed!");
    }

    public static void main(String[] args) {



        try {
            String homedirPath = System.getProperty("user.home");
            mRootDir = new File(new File(homedirPath), ".mondofs");

            File configFile = new File(mRootDir, "mondo.cfg");
            JsonReader reader = new JsonReader(new FileReader(configFile));
            mConfigFile = mGson.fromJson(reader, ConfigFile.class);


            FileSystemProvider mondoFileSystemProvider = getMondoFileSystemProvider();

            final String authority = mConfigFile.mount.replaceAll("/", "");
            URI uri = URI.create("mondofs://" + authority);
            FileSystem filesystem = mondoFileSystemProvider.newFileSystem(uri, null);

            final Path mountpoint = Paths.get(mConfigFile.mount);
            JavaFS.mount(filesystem, mountpoint, false, false);

            Runtime.getRuntime().addShutdownHook(new ShutdownThread());
            mRunning = true;
            while(mRunning) {
                Thread.sleep(500);
            }

            System.out.println("Unmounting...");
            JavaFS.unmount(mountpoint);

        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {

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
