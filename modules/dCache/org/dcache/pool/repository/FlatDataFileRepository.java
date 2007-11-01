package org.dcache.pool.repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.PnfsId;

/**
 * A data file repository layout keeping all files in a single
 * subdirectory called "data".
 */
public class FlatDataFileRepository implements DataFileRepository
{
    private final File _dataDir;
    
    public FlatDataFileRepository(File baseDir) throws FileNotFoundException
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
        return 0;// return _dataDir.getFreeSpace();
    }

    public long getTotalSpace() 
    {
        return 0;// return _dataDir.getTotalSpace();
    }
}
