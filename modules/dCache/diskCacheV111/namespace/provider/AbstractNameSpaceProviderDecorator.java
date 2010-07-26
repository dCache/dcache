package diskCacheV111.namespace.provider;

import java.util.Set;
import java.util.List;
import java.io.File;
import java.io.FileFilter;
import javax.security.auth.Subject;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.namespace.NameSpaceProvider;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.util.Checksum;
import org.dcache.util.Glob;
import org.dcache.util.Interval;
import org.dcache.vehicles.FileAttributes;

/**
 * Base class for decorators of NameSpaceProvider. All methods call
 * through to the decorated object.
 */
public class AbstractNameSpaceProviderDecorator
    implements NameSpaceProvider
{
    protected NameSpaceProvider _inner;

    public AbstractNameSpaceProviderDecorator(NameSpaceProvider inner)
    {
        _inner = inner;
    }

    @Override
    public PnfsId createEntry(Subject subject, String path, int uid, int gid, int mode, boolean isDirectory)
        throws CacheException
    {
        return _inner.createEntry(subject, path, uid, gid, mode, isDirectory);
    }

    @Override
    public void deleteEntry(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        _inner.deleteEntry(subject, pnfsId);
    }

    @Override
    public void deleteEntry(Subject subject, String path)
        throws CacheException
    {
        _inner.deleteEntry(subject, path);
    }

    @Override
    public void renameEntry(Subject subject, PnfsId pnfsId,
                            String newName, boolean overwrite)
        throws CacheException
    {
        _inner.renameEntry(subject, pnfsId, newName, overwrite);
    }

    @Override
    public String pnfsidToPath(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        return _inner.pnfsidToPath(subject, pnfsId);
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLinks)
        throws CacheException
    {
        return _inner.pathToPnfsid(subject, path, followLinks);
    }

    @Override
    public PnfsId getParentOf(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        return _inner.getParentOf(subject, pnfsId);
    }

    @Override
    public String[] getFileAttributeList(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        return _inner.getFileAttributeList(subject, pnfsId);
    }

    @Override
    public Object getFileAttribute(Subject subject, PnfsId pnfsId, String attribute)
        throws CacheException
    {
        return _inner.getFileAttribute(subject, pnfsId, attribute);
    }

    @Override
    public void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute)
        throws CacheException
    {
        _inner.removeFileAttribute(subject, pnfsId, attribute);
    }

    @Override
    public void setFileAttribute(Subject subject, PnfsId pnfsId, String attribute, Object data)
        throws CacheException
    {
        _inner.setFileAttribute(subject, pnfsId, attribute, data);
    }

    @Override
    public void addChecksum(Subject subject, PnfsId pnfsId, int type, String value)
        throws CacheException
    {
        _inner.addChecksum(subject, pnfsId, type, value);
    }

    @Override
    public String getChecksum(Subject subject, PnfsId pnfsId, int type)
        throws CacheException
    {
        return _inner.getChecksum(subject, pnfsId, type);
    }

    @Override
    public void removeChecksum(Subject subject, PnfsId pnfsId, int type)
        throws CacheException
    {
        _inner.removeChecksum(subject, pnfsId, type);
    }

    @Override
    public int[] listChecksumTypes(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        return _inner.listChecksumTypes(subject, pnfsId);
    }

    @Override
    public Set<Checksum> getChecksums(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        return _inner.getChecksums(subject, pnfsId);
    }

    @Override
    public void setStorageInfo(Subject subject, PnfsId pnfsId, StorageInfo storageInfo, int mode)
        throws CacheException
    {
        _inner.setStorageInfo(subject, pnfsId, storageInfo, mode);
    }

    @Override
    public void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation)
        throws CacheException
    {
        _inner.addCacheLocation(subject, pnfsId, cacheLocation);
    }

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        return _inner.getCacheLocation(subject, pnfsId);
    }

    @Override
    public void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation, boolean removeIfLast)
        throws CacheException
    {
        _inner.clearCacheLocation(subject, pnfsId, cacheLocation, removeIfLast);
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
                                            Set<FileAttribute> attr)
        throws CacheException
    {
        return _inner.getFileAttributes(subject, pnfsId, attr);
    }

    @Override
    public void setFileAttributes(Subject subject, PnfsId pnfsId, FileAttributes attr)
            throws CacheException
    {
        _inner.setFileAttributes(subject, pnfsId, attr);
    }

    @Override
    public void list(Subject subject, String path, Glob glob, Interval range,
                     Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException
    {
        _inner.list(subject, path, glob, range, attrs, handler);
    }
}