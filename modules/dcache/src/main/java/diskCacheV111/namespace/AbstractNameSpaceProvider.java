package diskCacheV111.namespace;

import java.util.Set;
import java.util.List;
import java.io.File;
import java.io.FileFilter;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;

import javax.security.auth.Subject;
import org.dcache.vehicles.FileAttributes;

import com.google.common.collect.Range;

public class AbstractNameSpaceProvider
    implements NameSpaceProvider
{
    @Override
    public PnfsId createEntry(Subject subject, String path, int uid, int gid, int mode, boolean isDirectory)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteEntry(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteEntry(Subject subject, String path)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameEntry(Subject subject, PnfsId pnfsId,
                            String newName, boolean overwrite)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String pnfsidToPath(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLinks)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId getParentOf(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeChecksum(Subject subject, PnfsId pnfsId, ChecksumType type)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStorageInfo(Subject subject, PnfsId pnfsId, StorageInfo storageInfo, int mode)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation, boolean removeIfLast)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
                                            Set<FileAttribute> attr)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFileAttributes(Subject subject, PnfsId pnfsId, FileAttributes attr)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void list(Subject subject, String path, Glob glob, Range<Integer> range,
                     Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }
}