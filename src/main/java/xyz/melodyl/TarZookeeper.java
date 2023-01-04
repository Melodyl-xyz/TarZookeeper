package xyz.melodyl;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;

import java.io.*;
import java.util.*;

public class TarZookeeper {
    private static final String PREFIX_SNAPSHOT = "snapshot";
    private static final String PREFIX_TXN = "log";
    private static final String SNAPSHOT_DIR_KEY = "dataDir";
    private static final String TXN_DIR_KEY = "dataLogDir";
    private static final String TAR_FILENAME = "data.tar.gz";

    static class TarZookeeperConfig {
        String snapDir;
        String txnDir;

        void valid() {
            if (snapDir == null || snapDir.length() == 0) {
                throw new IllegalArgumentException("snapDir cannot be nil or empty");
            }

            if (txnDir == null || txnDir.length() == 0) {
                throw new IllegalArgumentException("txnDir cannot be nil or empty");
            }
        }

        @Override
        public String toString() {
            return "TarZookeeperConfig{" +
                    "snapDir='" + snapDir + '\'' +
                    ", txnDir='" + txnDir + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 1 && "help".equals(args[0])) {
            printUsageThenExit();
            return;
        }

        String configDir = System.getProperty("configPath", "");
        String txnDir = System.getProperty("txnDir", "");
        String snapDir = System.getProperty("snapDir", "");
        String tarDir = System.getProperty("tarDir", "./");
        int snapSavedNum = Integer.parseInt(System.getProperty("snapNum", "1"));
        if (snapSavedNum < 1) {
            throw new IllegalArgumentException("snapNum should be greater than or equal to 1");
        }

        TarZookeeperConfig tzc = initialFromConfig(configDir);
        if (txnDir.length() > 0) {
            tzc.txnDir = txnDir;
        }
        if (snapDir.length() > 0) {
            tzc.snapDir = snapDir;
        }
        tzc.valid();
        System.out.println("tar config: " + tzc.toString());

        FileTxnSnapLog tsf = new FileTxnSnapLog(new File(tzc.txnDir), new File(tzc.snapDir));
        List<File> savedSnap = selectSnapshot(tsf, snapSavedNum);
        File[] savedTxn = selectTxn(tsf, savedSnap);
        if (savedTxn == null) {
            System.out.println("no txn file found, skip compress.");
            return;
        }
        System.out.printf("select snap cnt:%d, txn cnt: %d\n", savedSnap.size(), savedTxn.length);

        compress(savedSnap, savedTxn, tarDir);
    }

    public static TarZookeeperConfig initialFromConfig(String configDir) throws IOException {
        if (configDir.length() == 0) {
            return new TarZookeeperConfig();
        }

        Properties cfg = new Properties();
        try (FileInputStream in = new FileInputStream(new File(configDir))) {
            cfg.load(in);
        }

        TarZookeeperConfig tzc = new TarZookeeperConfig();
        for (Map.Entry<Object, Object> entry : cfg.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals(SNAPSHOT_DIR_KEY)) {
                tzc.snapDir = value;
            } else if (key.equals(TXN_DIR_KEY)) {
                tzc.txnDir = value;
            }
        }
        return tzc;
    }

    public static List<File> selectSnapshot(FileTxnSnapLog tsf, int snapSavedNum) throws IOException {
        return tsf.findNRecentSnapshots(snapSavedNum);
    }

    public static File[] selectTxn(FileTxnSnapLog tsf, List<File> savedSnapshot) {
        int numSnaps = savedSnapshot.size();
        File[] savedTxn;
        if (numSnaps > 0) {
            long leastSnapZxid = Util.getZxidFromName(savedSnapshot.get(numSnaps - 1).getName(), PREFIX_SNAPSHOT);
            savedTxn = tsf.getSnapshotLogs(leastSnapZxid);
        } else {
            class PrefixFileFilter implements FileFilter {
                private final String prefix;

                PrefixFileFilter(String prefix) {
                    this.prefix = prefix;
                }

                public boolean accept(File f) {
                    return f.getName().startsWith(prefix + ".");
                }
            }

            savedTxn = tsf.getDataDir().listFiles(new PrefixFileFilter(PREFIX_TXN));
            if (savedTxn == null || savedTxn.length == 0) {
                return null;
            }
        }
        return savedTxn;
    }

    public static void compress(List<File> snap, File[] txn, String tarDir) throws IOException {
        File tarFile = new File(tarDir, TAR_FILENAME);
        FileOutputStream fOut = new FileOutputStream(tarFile);
        BufferedOutputStream bOut = new BufferedOutputStream(fOut);
        GzipCompressorOutputStream gOut = new GzipCompressorOutputStream(bOut);
        TarArchiveOutputStream tOut = new TarArchiveOutputStream(gOut);
        tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

        for(File f : snap) {
            System.out.printf("compress snap:%s into %s\n", f.getName(), tarFile.getAbsolutePath());
            addFileToArchive(f, tOut);
        }

        for(File f : txn) {
            System.out.printf("compress txn:%s into %s\n", f.getName(), tarFile.getAbsolutePath());
            addFileToArchive(f, tOut);
        }

        tOut.close();
        gOut.close();
        bOut.close();
        fOut.close();
    }

    public static void addFileToArchive(File f, TarArchiveOutputStream tOut) throws IOException {
        tOut.putArchiveEntry(new TarArchiveEntry(f, f.getName()));
        FileInputStream fIn = new FileInputStream(f);
        BufferedInputStream bIn = new BufferedInputStream(fIn);
        IOUtils.copy(bIn, tOut);
        tOut.closeArchiveEntry();
        bIn.close();
        fIn.close();
    }


    private static void printUsageThenExit() {
        printUsage();
        System.exit(1);
    }

    static void printUsage() {
        System.out.println("Usage:");
        System.out.println("java -DconfigPath=[configPath] " +
                "-DtxnDir=[txnDir] -DsnapDir=[snapDir] -DtarDir=[tarDir] " +
                "-DsnapCount=[snapCount] -jar tar-zookeeper.jar");
        System.out.println("\tconfigPath -- path to the zoo.cfg file");
        System.out.println("\ttxnDir -- path to the txn directory");
        System.out.println("\tsnapDir -- path to the snap directory");
        System.out.println("\ttarDir -- path to the compress directory, default is ./");
        System.out.println("\tsnapNum -- the number of snaps you want");
    }
}
