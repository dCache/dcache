package org.dcache.pool.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

public class MetaDataCopyTool
{
    private static final Logger _log =
        LoggerFactory.getLogger(MetaDataCopyTool.class);

    static ReplicaStore createStore(Class<? extends ReplicaStore> clazz,
                                    FileStore fileStore, Path poolDir,
                                    String poolName, boolean readOnly)
        throws NoSuchMethodException, InstantiationException,
               IllegalAccessException, InvocationTargetException
    {
        Constructor<? extends ReplicaStore> constructor =
            clazz.getConstructor(FileStore.class, Path.class, String.class, Boolean.TYPE);
        return constructor.newInstance(fileStore, poolDir, poolName, readOnly);
    }

    public static void main(String[] args)
        throws Exception
    {
        if (args.length != 4) {
            System.err.println("Synopsis: MetaDataCopyTool DIR NAME FROM TO");
            System.err.println();
            System.err.println("Where DIR is the pool directory, NAME is the ");
            System.err.println("pool name, and FROM and TO are meta data store");
            System.err.println("class names.");
            System.exit(1);
        }

        Path poolDir = FileSystems.getDefault().getPath(args[0]);
        String poolName = args[1];
        FileStore fromFileStore = new DummyFileStore(DummyFileStore.Mode.ALL_EXIST);
        FileStore toFileStore = new DummyFileStore(DummyFileStore.Mode.NONE_EXIST);
        try (ReplicaStore fromStore =
                     createStore(Class.forName(args[2]).asSubclass(ReplicaStore.class), fromFileStore, poolDir, poolName, true);
             ReplicaStore toStore =
                     createStore(Class.forName(args[3]).asSubclass(ReplicaStore.class), toFileStore, poolDir, poolName, false)) {
            fromStore.init();
            toStore.init();

            if (!toStore.index(ReplicaStore.IndexOption.META_ONLY).isEmpty()) {
                System.err.println("ERROR: Target store is not empty");
                System.exit(1);
            }

            Collection<PnfsId> ids = fromStore.index(ReplicaStore.IndexOption.META_ONLY);
            int size = ids.size();
            int count = 1;
            for (PnfsId id : ids) {
                _log.info("Copying {} ({} of {})", id, count, size);
                ReplicaRecord entry = fromStore.get(id);
                if (entry == null) {
                    System.err.println("Failed to load " + id);
                    System.exit(1);
                }
                toStore.create(id, EnumSet.noneOf(Repository.OpenFlags.class)).update(r -> {
                    /* NOTE: We do not copy the last access time, as this is currently stored
                     * as the last modification time on the data file. If we at some point move
                     * the last access time into the meta data, this has to be updated here.
                     */
                    r.setState(entry.getState());
                    for (StickyRecord s : entry.stickyRecords()) {
                        r.setSticky(s.owner(), s.expire(), true);
                    }
                    r.setFileAttributes(entry.getFileAttributes());
                    return null;
                });
                count++;
            }
        }
    }
}
