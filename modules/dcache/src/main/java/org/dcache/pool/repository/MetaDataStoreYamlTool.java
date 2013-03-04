package org.dcache.pool.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

public class MetaDataStoreYamlTool
{
    private final static Logger _log =
        LoggerFactory.getLogger(MetaDataStoreYamlTool.class);

    static MetaDataStore createStore(Class<? extends MetaDataStore> clazz,
                                     FileStore fileStore, File poolDir)
        throws NoSuchMethodException, InstantiationException,
               IllegalAccessException, InvocationTargetException
    {
        Constructor<? extends MetaDataStore> constructor =
            clazz.getConstructor(FileStore.class, File.class, Boolean.TYPE);
        return constructor.newInstance(fileStore, poolDir, true);
    }

    public static void main(String[] args)
        throws Exception
    {
        if (args.length != 2) {
            System.err.println("Synopsis: MetaDataStoreCopyTool DIR TYPE");
            System.err.println();
            System.err.println("Where DIR is the pool directory and TYPE is the meta");
            System.err.println("data store class.");
            System.exit(1);
        }

        File poolDir = new File(args[0]);
        FileStore fileStore = new FlatFileStore(poolDir);
        MetaDataStore metaStore =
            createStore(Class.forName(args[1]).asSubclass(MetaDataStore.class), fileStore, poolDir);

        PrintWriter out = new PrintWriter(System.out);
        PrintWriter error = new PrintWriter(System.err);
        for (PnfsId id: metaStore.list()) {
            try {
                MetaDataRecord record = metaStore.get(id);
                StorageInfo info = record.getStorageInfo();

                out.format("%s:\n", id);
                out.format("  state: %s\n", record.getState());
                out.format("  sticky:\n");
                for (StickyRecord sticky: record.stickyRecords()) {
                    out.format("    %s: %d\n", sticky.owner(), sticky.expire());
                }
                if (info != null) {
                    out.format("  storageclass: %s\n", info.getStorageClass());
                    out.format("  cacheclass: %s\n", info.getCacheClass());
                    out.format("  bitfileid: %s\n", info.getBitfileId());
                    out.format("  locations:\n");
                    for (URI location: info.locations()) {
                        out.format("    - %s\n", location);
                    }
                    out.format("  hsm: %s\n", info.getHsm());
                    out.format("  filesize: %s\n", info.getFileSize());
                    out.format("  map:\n");
                    for (Map.Entry<String,String> entry : info.getMap().entrySet()) {
                        out.format("    %s: %s\n", entry.getKey(), entry.getValue());
                    }
                    out.format("  retentionpolicy: %s\n", info.getRetentionPolicy());
                    out.format("  accesslatency: %s\n", info.getAccessLatency());
                }
            } catch (Exception e) {
                error.println("Failed to read " + id + ": " + e.getMessage());
            }
        }
        out.flush();
        error.flush();
   }
}
