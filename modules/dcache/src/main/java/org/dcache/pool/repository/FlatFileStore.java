package org.dcache.pool.repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
        if (!baseDir.isDirectory()) {
            throw new FileNotFoundException("No such directory: " + baseDir);
        }

        _dataDir = new File(baseDir, "data");
        if (!_dataDir.exists()) {
            if (!_dataDir.mkdir()) {
                throw new FileNotFoundException("Failed to create directory: " + _dataDir);
            }
        } else if (!_dataDir.isDirectory()) {
            throw new FileNotFoundException("No such directory: " + _dataDir);
        }
    }

    /**
     * Returns a human readable description of the file store.
     */
    public String toString()
    {
        return _dataDir.getPath();
    }

    @Override
    public File get(PnfsId id)
    {
        return new File(_dataDir, id.toString());
    }

    @Override
    public Set<PnfsId> index()
    {
        String[] files = _dataDir.list();
        Set<PnfsId> ids = new HashSet<>(files.length);

        for (String name : files) {
            try {
                ids.add(new PnfsId(name));
            } catch (IllegalArgumentException e) {
                // data file contains foreign key
            }
        }

        return ids;
    }

    @Override
    public long getFreeSpace()
    {
        return _dataDir.getUsableSpace();
    }

    @Override
    public long getTotalSpace()
    {
        return _dataDir.getTotalSpace();
    }

    @Override
    public boolean isOk()
    {
        try {
            File tmp = new File(_dataDir, ".repository_is_ok");
            tmp.delete();
            tmp.deleteOnExit();

            if (!tmp.createNewFile()) {
                return false;
            }

            if (!tmp.exists()) {
                return false;
            }

            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
