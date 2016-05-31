package org.dcache.pool.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import diskCacheV111.util.PnfsId;

public class MetaDataStoreCopyTool2
{
    private static final Logger _log =
        LoggerFactory.getLogger(MetaDataStoreCopyTool2.class);

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
        FileStore fileStore = new DummyFileStore();
        try (MetaDataStore fromStore =
                     createStore(Class.forName(args[1]).asSubclass(MetaDataStore.class), fileStore, poolDir, true);
             MetaDataStore toStore =
                     createStore(Class.forName(args[2]).asSubclass(MetaDataStore.class), fileStore, poolDir, false)) {
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
                toStore.copy(entry);
                count++;
            }
        }
    }
}
