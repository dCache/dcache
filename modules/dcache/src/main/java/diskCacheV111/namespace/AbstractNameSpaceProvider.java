package diskCacheV111.namespace;

import com.google.common.collect.Range;

import javax.security.auth.Subject;

import java.util.List;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

public class AbstractNameSpaceProvider
    implements NameSpaceProvider
{
    @Override
    public FileAttributes createFile(Subject subject, String path, int uid, int gid, int mode,
                                     Set<FileAttribute> requestedAttributes)
        throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId createDirectory(Subject subject, String path, int uid, int gid, int mode)
            throws CacheException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId createSymLink(Subject subject, String path, String dest, int uid, int gid)
            throws CacheException {
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
    public FileAttributes setFileAttributes(Subject subject, PnfsId pnfsId,
            FileAttributes attr, Set<FileAttribute> acquire) throws CacheException
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

    @Override
    public FsPath createUploadPath(Subject subject, FsPath path, int uid, int gid, int mode,
                                   Long size,
                                   AccessLatency al, RetentionPolicy rp, String spaceToken,
                                   Set<CreateOption> options) throws CacheException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PnfsId commitUpload(Subject subject, FsPath uploadPath, FsPath pnfsPath, Set<CreateOption> options)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelUpload(Subject subject, FsPath uploadPath, FsPath path) throws CacheException
    {
        throw new UnsupportedOperationException();
    }
}
