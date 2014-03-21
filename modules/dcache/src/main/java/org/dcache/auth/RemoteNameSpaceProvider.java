package org.dcache.auth;

import com.google.common.collect.Range;

import javax.security.auth.Subject;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PnfsCancelUpload;
import diskCacheV111.vehicles.PnfsCommitUpload;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsCreateUploadPath;
import diskCacheV111.vehicles.PnfsFlagMessage;

import org.dcache.namespace.CreateOption;
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
    public FileAttributes createFile(Subject subject, String path, int uid, int gid, int mode,
                                     Set<FileAttribute> requestedAttributes)
            throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        PnfsCreateEntryMessage returnMsg =
                pnfs.pnfsRequest(new PnfsCreateEntryMessage(path, uid, gid, mode, requestedAttributes));
        return returnMsg.getFileAttributes();
    }

    @Override
    public PnfsId createDirectory(Subject subject, String path, int uid, int gid, int mode)
            throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);

        PnfsCreateEntryMessage returnMsg = pnfs.createPnfsDirectory(path, uid, gid, mode);

        return returnMsg.getPnfsId();
    }

    @Override
    public PnfsId createSymLink(Subject subject, String path, String dest,
            int uid, int gid) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);

        PnfsCreateEntryMessage returnMsg = pnfs.createSymLink(path, dest, uid, gid);

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
        return pnfs.getPathByPnfsId(id).toString();
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
    public FileAttributes setFileAttributes(Subject subject, PnfsId id,
            FileAttributes attr, Set<FileAttribute> acquire) throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        return pnfs.setFileAttributes(id, attr, acquire);
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

    @Override
    public FsPath createUploadPath(Subject subject, FsPath path, int uid, int gid, int mode,
                                   Long size, AccessLatency al, RetentionPolicy rp, String spaceToken,
                                   Set<CreateOption> options)
            throws CacheException
    {
        PnfsCreateUploadPath msg = new PnfsCreateUploadPath(subject, path,
                                                            uid, gid, mode, size,
                                                            al, rp, spaceToken,
                                                            options);
        return _pnfs.pnfsRequest(msg).getUploadPath();
    }

    @Override
    public PnfsId commitUpload(Subject subject, FsPath uploadPath, FsPath pnfsPath, Set<CreateOption> options)
            throws CacheException
    {
        PnfsCommitUpload msg = new PnfsCommitUpload(subject,
                                                    uploadPath,
                                                    pnfsPath,
                                                    options,
                                                    EnumSet.noneOf(FileAttribute.class));
        return _pnfs.pnfsRequest(msg).getPnfsId();
    }

    @Override
    public void cancelUpload(Subject subject, FsPath uploadPath, FsPath path) throws CacheException
    {
        _pnfs.pnfsRequest(new PnfsCancelUpload(subject, uploadPath, path));
    }
}
