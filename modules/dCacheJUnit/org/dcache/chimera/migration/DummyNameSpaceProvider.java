package org.dcache.chimera.migration;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.util.Checksum;
import org.dcache.util.Glob;
import org.dcache.util.Interval;
import org.dcache.vehicles.FileAttributes;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

/**
 * The DummyNameSpaceProvider allows storing of StorageInfo and
 * FileAttributes against arbitrary PnfsId values. These values may be
 * retrieved later.
 */
public class DummyNameSpaceProvider implements NameSpaceProvider {
    Map<PnfsId, StorageInfo> _storageInfo = new HashMap<PnfsId, StorageInfo>();
    Map<PnfsId, FileAttributes> _fileAttributes = new HashMap<PnfsId, FileAttributes>();

    @Override
    public void addCacheLocation( Subject s, PnfsId p, String c)
            throws CacheException {
        fail( "not implemented");
    }

    @Override
    public void addChecksum( Subject subject, PnfsId pnfsId, int type,
                             String value) throws CacheException {
        fail( "not implemented");
    }

    @Override
    public void clearCacheLocation( Subject subject, PnfsId pnfsId,
                                    String cacheLocation, boolean removeIfLast)
            throws CacheException {
        fail( "not implemented");
    }

    @Override
    public PnfsId createEntry( Subject subject, String path,
                               int uid, int gid, int mode, boolean isDirectory)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public void deleteEntry( Subject subject, PnfsId pnfsId)
            throws CacheException {
        fail( "not implemented");
    }

    @Override
    public void deleteEntry( Subject subject, String path)
            throws CacheException {
        fail( "not implemented");
    }

    @Override
    public List<String> getCacheLocation( Subject subject, PnfsId pnfsId)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public String getChecksum( Subject subject, PnfsId pnfsId, int type)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public Set<Checksum> getChecksums( Subject subject, PnfsId pnfsId)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public Object getFileAttribute( Subject subject, PnfsId pnfsId,
                                    String attribute) throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public String[] getFileAttributeList( Subject subject, PnfsId pnfsId)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public FileAttributes getFileAttributes( Subject subject, PnfsId pnfsId,
                                             Set<FileAttribute> attr)
            throws CacheException {
        FileAttributes attributes =  _fileAttributes.get(pnfsId);
        if (_storageInfo.containsKey(pnfsId)) {
            if (attributes == null) {
                attributes = new FileAttributes();
            }
            attributes.setStorageInfo(_storageInfo.get(pnfsId));
        }
        return attributes;
    }

    @Override
    public PnfsId getParentOf( Subject subject, PnfsId pnfsId)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public void list( Subject subject, String path, Glob glob, Interval range,
                      Set<FileAttribute> attrs, ListHandler handler)
            throws CacheException {
        fail( "not implemented");
    }

    @Override
    public int[] listChecksumTypes( Subject subject, PnfsId pnfsId)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public PnfsId pathToPnfsid( Subject subject, String path,
                                boolean followLinks) throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public String pnfsidToPath( Subject subject, PnfsId pnfsId)
            throws CacheException {
        fail( "not implemented");
        return null;
    }

    @Override
    public void removeChecksum( Subject subject, PnfsId pnfsId, int type)
            throws CacheException {
        fail( "not implemented");
    }

    @Override
    public void removeFileAttribute( Subject subject, PnfsId pnfsId,
                                     String attribute) throws CacheException {
        fail( "not implemented");
    }

    @Override
    public void renameEntry( Subject subject, PnfsId pnfsId, String newName,
                             boolean overwrite) throws CacheException {
        fail( "not implemented");
    }

    @Override
    public void setFileAttribute( Subject subject, PnfsId pnfsId,
                                  String attribute, Object data)
            throws CacheException {
        fail( "not implemented");
    }

    @Override
    public void setFileAttributes( Subject subject, PnfsId pnfsId,
                                   FileAttributes attr) throws CacheException {
        _fileAttributes.put(pnfsId, attr);
    }

    @Override
    public void setStorageInfo( Subject subject, PnfsId pnfsId,
                                StorageInfo storageInfo, int mode)
            throws CacheException {
        _storageInfo.put( pnfsId, storageInfo);
    }

}
