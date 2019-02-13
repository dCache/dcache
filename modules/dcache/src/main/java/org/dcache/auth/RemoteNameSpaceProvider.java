package org.dcache.auth;

import com.google.common.collect.Range;

import javax.security.auth.Subject;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.Collection;
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
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCommitUpload;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsCreateUploadPath;
import diskCacheV111.vehicles.PnfsFlagMessage;

import org.dcache.auth.attributes.Restrictions;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;

import static diskCacheV111.vehicles.PnfsFlagMessage.FlagOperation.REMOVE;
import static org.dcache.namespace.FileType.DIR;
import static org.dcache.namespace.FileType.REGULAR;

/**
 * The RemoteNameSpaceProvider uses the PnfsManager client stub to provide
 * an implementation of the NameSpaceProvider interface.  This implementation
 * is thread-safe.
 */
public class RemoteNameSpaceProvider implements NameSpaceProvider {
    private final PnfsHandler _pnfs;
    private final ListDirectoryHandler _handler;


    public RemoteNameSpaceProvider(PnfsHandler pnfsHandler,
                                   ListDirectoryHandler listHandler) {
        _pnfs = pnfsHandler;
        _handler = listHandler;
    }

    public RemoteNameSpaceProvider(PnfsHandler pnfsHandler) {
        this(pnfsHandler, new ListDirectoryHandler(pnfsHandler));
    }

    @Override
    public FileAttributes createFile(Subject subject, String path,
                                     FileAttributes assignAttributes, Set<FileAttribute> requestedAttributes)
            throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        assignAttributes.setFileType(REGULAR);
        PnfsCreateEntryMessage message = new PnfsCreateEntryMessage(path,
                assignAttributes, requestedAttributes);
        return pnfs.request(message).getFileAttributes();
    }

    @Override
    public PnfsId createDirectory(Subject subject, String path, FileAttributes attributes)
            throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        attributes.setFileType(DIR);
        PnfsCreateEntryMessage returnMsg = pnfs.createPnfsDirectory(path, attributes);
        return returnMsg.getPnfsId();
    }

    @Override
    public PnfsId createSymLink(Subject subject, String path, String dest,
                                FileAttributes attributes) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());

        PnfsCreateEntryMessage returnMsg = pnfs.createSymLink(path, dest, attributes);

        return returnMsg.getPnfsId();
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
                                      PnfsId id, Set<FileAttribute> attr) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.deletePnfsEntry(id, null, allowed, attr);
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
                                      String path, Set<FileAttribute> attr) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.deletePnfsEntry(null, path, allowed, attr);
    }

    @Override
    public FileAttributes deleteEntry(Subject subject, Set<FileType> allowed,
                                      PnfsId pnfsId, String path, Set<FileAttribute> attr) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.deletePnfsEntry(pnfsId, path, allowed, attr);
    }

    @Override
    public void rename(Subject subject, PnfsId id, String sourcePath, String newName, boolean overwrite) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        pnfs.renameEntry(id, sourcePath, newName, overwrite);
    }

    @Override
    public String pnfsidToPath(Subject subject, PnfsId id) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.getPathByPnfsId(id).toString();
    }

    @Override
    public PnfsId pathToPnfsid(Subject subject, String path,
                               boolean followLinks) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.getPnfsIdByPath(path, followLinks);
    }

    @Override
    public Collection<Link> find(Subject subject, PnfsId id) throws CacheException {
        return new PnfsHandler(_pnfs, subject, Restrictions.none()).find(id);
    }

    @Override
    public void removeFileAttribute(Subject subject, PnfsId id,
                                    String attribute) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        pnfs.notify(new PnfsFlagMessage(id, attribute, REMOVE));
    }


    @Override
    public void removeChecksum(Subject subject, PnfsId id, ChecksumType type)
            throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        pnfs.removeChecksum(id, type);
    }

    @Override
    public void addCacheLocation(Subject subject, PnfsId id, String pool)
            throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        pnfs.addCacheLocation(id, pool);
    }

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId id)
            throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.getCacheLocations(id);
    }

    @Override
    public void clearCacheLocation(Subject subject, PnfsId id, String pool,
                                   boolean removeIfLast) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        pnfs.request(new PnfsClearCacheLocationMessage(id, pool, removeIfLast));
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId id,
                                            Set<FileAttribute> attr) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.getFileAttributes(id, attr);
    }

    @Override
    public FileAttributes setFileAttributes(Subject subject, PnfsId id,
                                            FileAttributes attr, Set<FileAttribute> acquire) throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject, Restrictions.none());
        return pnfs.setFileAttributes(id, attr, acquire);
    }

    @Override
    public void list(Subject subject, String path, Glob glob,
                     Range<Integer> range, Set<FileAttribute> attrs, ListHandler handler)
            throws CacheException {
        try (DirectoryStream<DirectoryEntry> stream = _handler.list(subject, Restrictions.none(), FsPath.create(path), glob, range, attrs)) {
            for (DirectoryEntry entry : stream) {
                handler.addEntry(entry.getName(), entry.getFileAttributes());
            }
        } catch (InterruptedException | IOException e) {
            throw new TimeoutCacheException(e.getMessage());
        }
    }

    @Override
    public FsPath createUploadPath(Subject subject, FsPath path, FsPath rootPath,
                                   Long size, AccessLatency al, RetentionPolicy rp, String spaceToken,
                                   Set<CreateOption> options)
            throws CacheException {
        PnfsCreateUploadPath msg = new PnfsCreateUploadPath(subject, Restrictions.none(), path, rootPath,
                size, al, rp, spaceToken, options);
        return _pnfs.request(msg).getUploadPath();
    }

    @Override
    public FileAttributes commitUpload(Subject subject, FsPath uploadPath, FsPath pnfsPath,
                                       Set<CreateOption> options, Set<FileAttribute> attributes)
            throws CacheException {
        PnfsCommitUpload msg = new PnfsCommitUpload(subject,
                Restrictions.none(),
                uploadPath,
                pnfsPath,
                options,
                attributes);
        return _pnfs.request(msg).getFileAttributes();
    }

    @Override
    public Collection<FileAttributes> cancelUpload(Subject subject, FsPath uploadPath, FsPath path,
                                                   Set<FileAttribute> requested, String explanation) throws CacheException {
        return _pnfs.request(new PnfsCancelUpload(subject, Restrictions.none(),
                uploadPath, path, requested, explanation)).getDeletedFiles();
    }
}
