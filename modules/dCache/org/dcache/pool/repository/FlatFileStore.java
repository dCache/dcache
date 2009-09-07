package org.dcache.pool.repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.PnfsId;

/**
 * A file store layout keeping all files in a single subdirectory
 * called "data".
 */
public class FlatFileStore implements FileStore
{
    private final File _dataDir;

    public FlatFileStore(File baseDir) throws FileNotFoundException
    {
        if (!baseDir.exists()) {
            throw new FileNotFoundException(baseDir + " does not exist.");
        }

        if (!baseDir.isDirectory()) {
            throw new FileNotFoundException(baseDir + " Not a directory.");
        }

        _dataDir = new File(baseDir, "data");

        if (!_dataDir.isDirectory()) {
            throw new FileNotFoundException(_dataDir + " does not exist or not a directory.");
        }
    }

    /**
     * Returns a human readable description of the file store.
     */
    public String toString()
    {
        return _dataDir.getPath();
    }

    public File get(PnfsId id)
    {
        return new File(_dataDir, id.toString());
    }

    public List<PnfsId> list()
    {
        String[] files = _dataDir.list();
        List<PnfsId> ids = new ArrayList<PnfsId>(files.length);

        for (String name : files) {
            try {
                ids.add(new PnfsId(name));
            } catch (IllegalArgumentException e) {
                // data file contains foreign key
            }
        }

        return ids;
    }

    public long getFreeSpace()
    {
        return _dataDir.getUsableSpace();
    }

    public long getTotalSpace()
    {
        return _dataDir.getTotalSpace();
    }

    public boolean isOk()
    {
        try {
            File tmp = new File(_dataDir, ".repository_is_ok");
            tmp.delete();
            tmp.deleteOnExit();

            if (!tmp.createNewFile())
                return false;

            if (!tmp.exists())
                return false;

            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
