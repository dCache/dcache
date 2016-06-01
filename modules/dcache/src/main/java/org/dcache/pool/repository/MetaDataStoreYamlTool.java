package org.dcache.pool.repository;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

public class MetaDataStoreYamlTool
{
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
        FileStore fileStore = new DummyFileStore();
        try (MetaDataStore metaStore =
                     createStore(Class.forName(args[1]).asSubclass(MetaDataStore.class), fileStore, poolDir)) {
            metaStore.init();

            PrintWriter out = new PrintWriter(System.out);
            PrintWriter error = new PrintWriter(System.err);
            for (PnfsId id : metaStore.index(MetaDataStore.IndexOption.META_ONLY)) {
                try {
                    MetaDataRecord record = metaStore.get(id);
                    if (record == null) {
                        continue;
                    }
                    FileAttributes attributes = record.getFileAttributes();

                    out.format("%s:\n", id);
                    out.format("  state: %s\n", record.getState());
                    out.format("  sticky:\n");
                    for (StickyRecord sticky : record.stickyRecords()) {
                        out.format("    %s: %d\n", sticky.owner(), sticky.expire());
                    }
                    if (attributes.isDefined(FileAttribute.STORAGECLASS)) {
                        out.format("  storageclass: %s\n", attributes.getStorageClass());
                    }
                    if (attributes.isDefined(FileAttribute.CACHECLASS)) {
                        out.format("  cacheclass: %s\n", attributes.getCacheClass());
                    }
                    if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
                        StorageInfo info = attributes.getStorageInfo();
                        out.format("  bitfileid: %s\n", info.getBitfileId());
                        out.format("  locations:\n");
                        for (URI location : info.locations()) {
                            out.format("    - %s\n", location);
                        }
                    }
                    if (attributes.isDefined(FileAttribute.HSM)) {
                        out.format("  hsm: %s\n", attributes.getHsm());
                    }
                    if (attributes.isDefined(FileAttribute.SIZE)) {
                        out.format("  filesize: %s\n", attributes.getSize());
                    }
                    if (attributes.isDefined(FileAttribute.FLAGS)) {
                        out.format("  map:\n");
                        for (Map.Entry<String, String> entry : attributes.getFlags().entrySet()) {
                            out.format("    %s: %s\n", entry.getKey(), entry.getValue());
                        }
                    }
                    if (attributes.isDefined(FileAttribute.RETENTION_POLICY)) {
                        out.format("  retentionpolicy: %s\n", attributes.getRetentionPolicy());
                    }
                    if (attributes.isDefined(FileAttribute.ACCESS_LATENCY)) {
                        out.format("  accesslatency: %s\n", attributes.getAccessLatency());
                    }
                } catch (CacheException e) {
                    error.println("Failed to read " + id + ": " + e.getMessage());
                }
            }
            out.flush();
            error.flush();
        }
    }

}
