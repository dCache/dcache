package org.dcache.auth;

import com.google.common.collect.Range;

import javax.security.auth.Subject;

import java.util.List;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;

import static diskCacheV111.vehicles.PnfsFlagMessage.FlagOperation.REMOVE;

/**
 * The RemoteNameSpaceProvider uses the PnfsManager client stub to provide
 * an implementation of the NameSpaceProvider interface.  This implementation
 * is thread-safe.
 */
public class RemoteNameSpaceProvider implements NameSpaceProvider
{
    private final PnfsHandler _pnfs;
    private final ListDirectoryHandler _handler;


    public RemoteNameSpaceProvider(PnfsHandler pnfsHandler,
            ListDirectoryHandler listHandler)
    {
        _pnfs = pnfsHandler;
        _handler = listHandler;
    }

    public RemoteNameSpaceProvider(PnfsHandler pnfsHandler)
    {
        this(pnfsHandler, new ListDirectoryHandler(pnfsHandler));
    }

    @Override
    public PnfsId createEntry(Subject subject, String path, int uid, int gid,
            int mode, boolean isDirectory) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);

        PnfsCreateEntryMessage returnMsg;

        if(isDirectory) {
            returnMsg = pnfs.createPnfsDirectory(path, uid, gid, mode);
        } else {
            returnMsg = pnfs.createPnfsEntry(path, uid, gid, mode);
        }

        return returnMsg.getPnfsId();
    }

    @Override
    public void deleteEntry(Subject subject, PnfsId id) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.deletePnfsEntry(id);
    }

    @Override
    public void deleteEntry(Subject subject, String path) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.deletePnfsEntry(path);
    }

    @Override
    public void renameEntry(Subject subject, PnfsId id, String newName,
            boolean overwrite) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.renameEntry(id, newName, overwrite);
    }

    @Override
    public String pnfsidToPath(Subject subject, PnfsId id) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        return pnfs.getPathByPnfsId(id);
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path,
            boolean followLinks) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        return pnfs.getPnfsIdByPath(path, followLinks);
    }

    @Override
    public PnfsId getParentOf(Subject subject, PnfsId id) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        return pnfs.getParentOf(id);
    }

    @Override
    public void removeFileAttribute(Subject subject, PnfsId id,
            String attribute) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.notify(new PnfsFlagMessage(id, attribute, REMOVE));
    }


    @Override
    public void removeChecksum(Subject subject, PnfsId id, ChecksumType type)
            throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.removeChecksum(id, type);
    }

    @Override
    public void addCacheLocation(Subject subject, PnfsId id, String pool)
            throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.addCacheLocation(id, pool);
    }

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId id)
            throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        return pnfs.getCacheLocations(id);
    }

    @Override
    public void clearCacheLocation(Subject subject, PnfsId id, String pool,
            boolean removeIfLast) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.clearCacheLocation(id, pool, removeIfLast);
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId id,
            Set<FileAttribute> attr) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        return pnfs.getFileAttributes(id, attr);
    }

    @Override
    public void setFileAttributes(Subject subject, PnfsId id,
            FileAttributes attr) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        pnfs.setFileAttributes(id, attr);
    }

    @Override
    public void list(Subject subject, String path, Glob glob,
            Range<Integer> range, Set<FileAttribute> attrs, ListHandler handler)
            throws CacheException
    {
        try (DirectoryStream stream = _handler.list(subject, new FsPath(path), glob, range, attrs)) {
            for (DirectoryEntry entry : stream) {
                handler.addEntry(entry.getName(), entry.getFileAttributes());
            }
        } catch (InterruptedException e) {
            throw new TimeoutCacheException(e.getMessage());
        }
    }
}
