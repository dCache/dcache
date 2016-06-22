package org.dcache.pool.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

public class MetaDataStoreCopyTool
{
    private static final Logger _log =
        LoggerFactory.getLogger(MetaDataStoreCopyTool.class);

    static MetaDataStore createStore(Class<? extends MetaDataStore> clazz,
                                     FileStore fileStore, File poolDir, boolean readOnly)
        throws NoSuchMethodException, InstantiationException,
               IllegalAccessException, InvocationTargetException
    {
        Constructor<? extends MetaDataStore> constructor =
            clazz.getConstructor(FileStore.class, File.class, Boolean.TYPE);
        return constructor.newInstance(fileStore, poolDir, readOnly);
    }

    public static void main(String[] args)
        throws Exception
    {
        if (args.length != 3) {
            System.err.println("Synopsis: MetaDataStoreCopyTool DIR FROM TO");
            System.err.println();
            System.err.println("Where DIR is the pool directory, and FROM and TO are");
            System.err.println("meta data store class names.");
            System.exit(1);
        }

        File poolDir = new File(args[0]);
        FileStore fromFileStore = new DummyFileStore(DummyFileStore.Mode.ALL_EXIST);
        FileStore toFileStore = new DummyFileStore(DummyFileStore.Mode.NONE_EXIST);
        try (MetaDataStore fromStore =
                     createStore(Class.forName(args[1]).asSubclass(MetaDataStore.class), fromFileStore, poolDir, true);
             MetaDataStore toStore =
                     createStore(Class.forName(args[2]).asSubclass(MetaDataStore.class), toFileStore, poolDir, false)) {
            fromStore.init();
            toStore.init();

            if (!toStore.index(MetaDataStore.IndexOption.META_ONLY).isEmpty()) {
                System.err.println("ERROR: Target store is not empty");
                System.exit(1);
            }

            Collection<PnfsId> ids = fromStore.index(MetaDataStore.IndexOption.META_ONLY);
            int size = ids.size();
            int count = 1;
            for (PnfsId id : ids) {
                _log.info("Copying {} ({} of {})", id, count, size);
                MetaDataRecord entry = fromStore.get(id);
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
