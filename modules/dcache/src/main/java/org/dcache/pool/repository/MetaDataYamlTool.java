package org.dcache.pool.repository;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

public class MetaDataYamlTool
{
    static ReplicaStore createStore(Class<? extends ReplicaStore> clazz,
                                    FileStore fileStore, String poolName, Path poolDir)
        throws NoSuchMethodException, InstantiationException,
               IllegalAccessException, InvocationTargetException
    {
        Constructor<? extends ReplicaStore> constructor =
            clazz.getConstructor(FileStore.class, Path.class, String.class, Boolean.TYPE);
        return constructor.newInstance(fileStore, poolDir, poolName, true);
    }

    public static void main(String[] args)
        throws Exception
    {
        if (args.length != 3) {
            System.err.println("Synopsis: MetaDataCopyTool NAME DIR TYPE");
            System.err.println();
            System.err.println("Where NAME is the pool name, DIR is the pool directory and TYPE is the meta");
            System.err.println("data store class.");
            System.exit(1);
        }

        String poolName = args[0];
        Path poolDir = Paths.get(args[1]);
        FileStore fileStore = new DummyFileStore(DummyFileStore.Mode.ALL_EXIST);
        try (ReplicaStore metaStore =
                     createStore(Class.forName(args[2]).asSubclass(ReplicaStore.class), fileStore, poolName, poolDir)) {
            metaStore.init();

            PrintWriter out = new PrintWriter(System.out);
            PrintWriter error = new PrintWriter(System.err);
            for (PnfsId id : metaStore.index(ReplicaStore.IndexOption.META_ONLY)) {
                try {
                    ReplicaRecord record = metaStore.get(id);
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
