package org.dcache.pool.repository;

import java.io.File;
import java.util.Collection;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import diskCacheV111.util.PnfsId;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class MetaDataStoreCopyTool
{
    private final static Logger _log =
        LoggerFactory.getLogger(MetaDataStoreCopyTool.class);

    static MetaDataStore createStore(Class<?> clazz,
                                     FileStore fileStore, File poolDir)
        throws NoSuchMethodException, InstantiationException,
               IllegalAccessException, InvocationTargetException
    {
        Constructor<?> constructor =
            clazz.getConstructor(FileStore.class, File.class);
        return (MetaDataStore) constructor.newInstance(fileStore, poolDir);
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
        FileStore fileStore = new FlatFileStore(poolDir);
        MetaDataStore fromStore =
            createStore(Class.forName(args[1]), fileStore, poolDir);
        MetaDataStore toStore =
            createStore(Class.forName(args[2]), fileStore, poolDir);

        if (!toStore.list().isEmpty()) {
            System.err.println("ERROR: Target store is not empty");
            System.exit(1);
        }

        Collection<PnfsId> ids = fromStore.list();
        int size = ids.size();
        int count = 1;
        for (PnfsId id: ids) {
            _log.info("Copying {} ({} of {})",
                      new Object[] { id, count, size });
            toStore.create(fromStore.get(id));
            count++;
        }
    }
}