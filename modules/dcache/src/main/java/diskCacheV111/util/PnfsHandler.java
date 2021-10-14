package diskCacheV111.util;

import static com.google.common.base.Preconditions.checkState;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileType.DIR;
import static org.dcache.namespace.FileType.LINK;

import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.namespace.NameSpaceProvider.Link;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetParentMessage;
import diskCacheV111.vehicles.PnfsListExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.PnfsReadExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsRemoveExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsRemoveLabelsMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import diskCacheV111.vehicles.PnfsWriteExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsWriteExtendedAttributesMessage.Mode;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsCreateSymLinkMessage;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsRemoveChecksumMessage;
import org.dcache.vehicles.PnfsSetFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PnfsHandler implements CellMessageSender {

    private final String _poolName;
    private static final long DEFAULT_PNFS_TIMEOUT = TimeUnit.MINUTES.toMillis(
          30);

    private final CellStub _cellStub;

    private Subject _subject;
    private Restriction _restriction;

    private static final Logger _logNameSpace =
          LoggerFactory.getLogger("logger.org.dcache.namespace."
                + PnfsHandler.class.getName());

    private static CellStub createStub(CellPath path) {
        CellStub stub = new CellStub();
        stub.setDestinationPath(path);
        stub.setTimeout(DEFAULT_PNFS_TIMEOUT);
        stub.setTimeoutUnit(TimeUnit.MILLISECONDS);
        stub.setFlags(CellEndpoint.SendFlag.RETRY_ON_NO_ROUTE_TO_CELL);
        return stub;
    }

    public PnfsHandler(CellEndpoint endpoint,
          CellPath pnfsManagerPath) {
        this(pnfsManagerPath);
        setCellEndpoint(endpoint);
    }

    public PnfsHandler(CellPath pnfsManagerPath) {
        this(createStub(pnfsManagerPath));
    }

    public PnfsHandler(CellPath pnfsManagerPath,
          String poolName) {
        this(createStub(pnfsManagerPath), poolName);
    }

    public PnfsHandler(CellStub stub) {
        this(stub, "<client>");
    }

    public PnfsHandler(CellStub stub, String poolName) {
        _cellStub = stub;
        _poolName = poolName;
    }

    /**
     * Copy constructor. The primary purpose is to create session specific PnfsHandlers with a
     * session specific subject. Notice that the CellStub is shared between the two handlers and
     * thus the timeout will always be the same.
     *
     * @param handler     The PnfsHandler to copy
     * @param subject     The Subject to apply to the copy
     * @param restriction The restriction to apply to the copy
     */
    public PnfsHandler(PnfsHandler handler, Subject subject, Restriction restriction) {
        _poolName = handler._poolName;
        _cellStub = handler._cellStub;
        _subject = subject;
        _restriction = restriction;
    }

    /**
     * Copy identity but not restriction.  The primary purpose is to provide a way to handle
     * error-recovery where, through restrictions, the user is not allowed to recover from the
     * problem.
     *
     * @param handler     The PnfsHandler to copy
     * @param restriction The restriction to apply to the copy
     */
    public PnfsHandler(PnfsHandler handler, Restriction restriction) {
        this(handler, handler._subject, restriction);
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _cellStub.setCellEndpoint(endpoint);
    }

    public void setSubject(Subject subject) {
        _subject = subject;
    }

    public void setRestriction(Restriction restriction) {
        _restriction = restriction;
    }

    /**
     * Sends a PnfsMessage to PnfsManager.
     */
    public void send(PnfsMessage msg) {
        checkState(_cellStub != null, "Missing endpoint");

        if (_subject != null) {
            msg.setSubject(_subject);
        }

        if (_restriction != null) {
            msg.setRestriction(_restriction);
        }

        _cellStub.notify(msg);
    }

    /**
     * Send a PnfsMessage to PnfsManager indicating that an expected response will be ignored after
     * some timeout has elapsed.  This method exists primarily to support legacy code; new code
     * should consider using the requestAsync method instead.
     *
     * @param msg     The message to send
     * @param timeout The duration, in milliseconds, after which any response will be ignored.
     */
    public void send(PnfsMessage msg, long timeout) {
        checkState(_cellStub != null, "Missing endpoint");

        if (_subject != null) {
            msg.setSubject(_subject);
        }

        if (_restriction != null) {
            msg.setRestriction(_restriction);
        }

        _cellStub.notify(msg, timeout);
    }

    /**
     * Sends a PnfsMessage notification to PnfsManager. No reply is expected for a notification and
     * no failure is reported if the message could not be delivered.
     */
    public void notify(PnfsMessage msg) {
        msg.setReplyRequired(false);
        send(msg);
    }

    public void clearCacheLocation(PnfsId id) {
        clearCacheLocation(id, false);
    }

    public void clearCacheLocation(PnfsId id, boolean removeIfLast) {
        notify(new PnfsClearCacheLocationMessage(id, _poolName, removeIfLast));
    }

    public void addCacheLocation(PnfsId id) throws CacheException {
        addCacheLocation(id, _poolName);
    }

    public void addCacheLocation(PnfsId id, String pool) throws CacheException {
        request(new PnfsAddCacheLocationMessage(id, pool));
    }

    public List<String> getCacheLocations(PnfsId pnfsId) throws CacheException {
        PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage(pnfsId);
        pnfsMessage = request(pnfsMessage);
        List<String> assumedLocations = pnfsMessage.getCacheLocations();

        if (assumedLocations == null) {
            return Collections.emptyList();
        } else {
            return assumedLocations;
        }
    }

    public List<String> getCacheLocationsByPath(String fileName) throws CacheException {
        PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage();
        pnfsMessage.setPnfsPath(fileName);
        pnfsMessage = request(pnfsMessage);
        List<String> assumedLocations = pnfsMessage.getCacheLocations();

        if (assumedLocations == null) {
            return Collections.emptyList();
        } else {
            return assumedLocations;
        }
    }

    /**
     * Sends a message to the request manager and blocks until a reply is received. In case of
     * errors in the reply, those are thrown as a CacheException. Timeouts and failure to send the
     * message to the PnfsManager are reported as a timeout CacheException.
     */
    public <T extends PnfsMessage> T request(T msg)
          throws CacheException {
        try {
            return CellStub.getMessage(requestAsync(msg));
        } catch (InterruptedException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Sending message to " + _cellStub.getDestinationPath() + " interrupted");
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage(), e);
        }
    }

    /**
     * Sends a message to the pnfs manager and returns a promise of a future reply.
     */
    public <T extends PnfsMessage> ListenableFuture<T> requestAsync(T msg) {
        checkState(_cellStub != null, "Missing endpoint");
        return requestAsync(msg, _cellStub.getTimeoutInMillis());
    }

    /**
     * Sends a message to the pnfs manager and returns a promise of a future reply.
     */
    public <T extends PnfsMessage> ListenableFuture<T> requestAsync(T msg, long timeout) {
        checkState(_cellStub != null, "Missing endpoint");

        msg.setReplyRequired(true);
        if (_subject != null) {
            msg.setSubject(_subject);
        }
        if (_restriction != null) {
            msg.setRestriction(_restriction);
        }
        return _cellStub.send(msg, timeout);
    }

    public PnfsCreateEntryMessage createPnfsDirectory(String path)
          throws CacheException {
        return request(new PnfsCreateEntryMessage(path, FileAttributes.ofFileType(DIR)));
    }

    public PnfsCreateEntryMessage createPnfsDirectory(String path,
          Set<FileAttribute> attributes) throws CacheException {
        return request(new PnfsCreateEntryMessage(path, FileAttributes.ofFileType(DIR),
              attributes));
    }

    public PnfsCreateEntryMessage createPnfsDirectory(String path,
          FileAttributes attributes) throws CacheException {
        return request(new PnfsCreateEntryMessage(path, attributes));
    }

    /**
     * Creates a directory and all its parent directories.
     * <p>
     * REVISIT: Should eventually be moved to PnfsManager with a flag in the PnfsCreateEntryMessage
     * indicating whether parent directories should be created.
     *
     * @returns the FileAttributes of <code>path</code>
     */
    public FileAttributes createDirectories(FsPath path)
          throws CacheException {
        PnfsCreateEntryMessage message;
        try {
            message = createPnfsDirectory(path.toString());
        } catch (FileNotFoundCacheException e) {
            createDirectories(path.parent());
            message = createPnfsDirectory(path.toString());
        }

        /* In case of incomplete create, delete the directory right
         * away. FIXME: PnfsManagerV3 has the exact opposite comment,
         * saying that lack of attributes is a non-error.
         */
        if (message.getFileAttributes() == null) {
            try {
                deletePnfsEntry(message.getPnfsId(), path.toString());
            } catch (FileNotFoundCacheException e) {
                // Already gone, so never mind
            } catch (CacheException e) {
                _logNameSpace.error(e.toString());
            }

            throw new CacheException("Failed to create directory: " + path);
        }

        return message.getFileAttributes();
    }

    public PnfsCreateEntryMessage createSymLink(String path, String dest,
          FileAttributes assignAttributes) throws CacheException {
        assignAttributes.setFileType(LINK);
        return request(new PnfsCreateSymLinkMessage(path, dest, assignAttributes));
    }

    public void renameEntry(PnfsId pnfsId, String path, String newName, boolean overwrite)
          throws CacheException {
        request(new PnfsRenameMessage(pnfsId, path, newName, overwrite));
    }

    public void renameEntry(String path, String newName, boolean overwrite)
          throws CacheException {
        request(new PnfsRenameMessage(path, newName, overwrite));
    }

    public PnfsCreateEntryMessage createPnfsEntry(String path,
          FileAttributes attributes) throws CacheException {
        return request(new PnfsCreateEntryMessage(path, attributes));
    }

    public Collection<Link> find(PnfsId pnfsId) throws CacheException {
        PnfsGetParentMessage response = request(new PnfsGetParentMessage(pnfsId));
        List<PnfsId> parents = response.getParents();
        List<String> names = response.getNames();
        int count = Math.min(parents.size(), names.size());
        List<Link> locations = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            locations.add(new Link(parents.get(i), names.get(i)));
        }
        return locations;
    }

    public PnfsId deletePnfsEntry(String path) throws CacheException {
        return deletePnfsEntry(path, EnumSet.allOf(FileType.class));
    }

    public PnfsId deletePnfsEntry(String path, Set<FileType> allowed)
          throws CacheException {
        FileAttributes attributes = deletePnfsEntry(null, path, allowed,
              EnumSet.of(PNFSID));
        return attributes.getPnfsId();
    }

    public void deletePnfsEntry(PnfsId pnfsid) throws CacheException {
        deletePnfsEntry(pnfsid, null, EnumSet.allOf(FileType.class),
              EnumSet.noneOf(FileAttribute.class));
    }

    public void deletePnfsEntry(PnfsId pnfsid, String path)
          throws CacheException {
        deletePnfsEntry(pnfsid, path, EnumSet.allOf(FileType.class),
              EnumSet.noneOf(FileAttribute.class));
    }

    public FileAttributes deletePnfsEntry(PnfsId pnfsid, String path, Set<FileType> allowed,
          Set<FileAttribute> attr)
          throws CacheException {
        return request(new PnfsDeleteEntryMessage(pnfsid, path, allowed, attr)).getFileAttributes();
    }

    /**
     * Getter for property __pnfsTimeout.
     *
     * @return Value of property __pnfsTimeout.
     */
    public long getPnfsTimeout() {
        return _cellStub.getTimeoutInMillis();
    }

    /**
     * Setter for property __pnfsTimeout.
     *
     * @param pnfsTimeout New value of property __pnfsTimeout.
     */
    public void setPnfsTimeout(long pnfsTimeout) {
        _cellStub.setTimeout(pnfsTimeout);
        _cellStub.setTimeoutUnit(TimeUnit.MILLISECONDS);
    }

    public void putPnfsFlag(PnfsId pnfsId, String flag, String value) {
        PnfsFlagMessage flagMessage =
              new PnfsFlagMessage(pnfsId, flag, PnfsFlagMessage.FlagOperation.SET);
        flagMessage.setReplyRequired(false);
        flagMessage.setValue(value);
        notify(flagMessage);
    }

    public void fileFlushed(PnfsId pnfsId, FileAttributes fileAttributes) throws CacheException {

        PoolFileFlushedMessage fileFlushedMessage = new PoolFileFlushedMessage(_poolName, pnfsId,
              fileAttributes);

        // throws exception if something goes wrong
        request(fileFlushedMessage);

    }

    /**
     * Get path corresponding to given pnfsid.
     *
     * @param pnfsID
     * @return path
     * @throws CacheException
     */
    public FsPath getPathByPnfsId(PnfsId pnfsID) throws CacheException {
        return FsPath.create(request(new PnfsMapPathMessage(pnfsID)).getPnfsPath());
    }

    /**
     * Get pnfsid corresponding to given path.
     *
     * @param path
     * @return pnfsid
     * @throws CacheException
     */
    public PnfsId getPnfsIdByPath(String path) throws CacheException {
        return getPnfsIdByPath(path, false);
    }

    /**
     * Get pnfsid corresponding to given path.
     *
     * @param path
     * @param resolve whether sym-links should be followed.  If set to false then the operation will
     *                fail if any path element is a symbolic link.
     * @return pnfsid
     * @throws CacheException
     */
    public PnfsId getPnfsIdByPath(String path, boolean resolve)
          throws CacheException {
        PnfsGetFileAttributes message = new PnfsGetFileAttributes(path,
              EnumSet.of(FileAttribute.PNFSID));
        message.setFollowSymlink(resolve);
        return request(message).getFileAttributes().getPnfsId();
    }


    /**
     * Remove the registered checksum (of the specified type) from the file with the given id.
     *
     * @param id
     * @param type
     */
    public void removeChecksum(PnfsId id, ChecksumType type) {
        notify(new PnfsRemoveChecksumMessage(id, type));
    }

    /**
     * Get file attributes. The PnfsManager is free to return fewer attributes than requested. If
     * <code>attr</code> is an empty array, file existence if checked.
     *
     * @param pnfsid
     * @param attr   array of requested attributes.
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(PnfsId pnfsid, Set<FileAttribute> attr)
          throws CacheException {
        return request(new PnfsGetFileAttributes(pnfsid, attr)).getFileAttributes();
    }

    /**
     * Get file attributes. The PnfsManager is free to return fewer attributes than requested. If
     * <code>attr</code> is an empty array, file existence if checked.
     *
     * @param pnfsid
     * @param attr        array of requested attributes.
     * @param mask        Additional AccessMask access rights to check
     * @param updateAtime update file's last access time
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(
          PnfsId pnfsid, Set<FileAttribute> attr, Set<AccessMask> mask, boolean updateAtime)
          throws CacheException {
        PnfsGetFileAttributes msg = new PnfsGetFileAttributes(pnfsid, attr);
        msg.setAccessMask(mask);
        msg.setUpdateAtime(updateAtime);
        return request(msg).getFileAttributes();
    }

    /**
     * Get file attributes. The PnfsManager is free to return fewer attributes than requested. If
     * <code>attr</code> is an empty array, file existence if checked.
     *
     * @param path
     * @param attr array of requested attributes.
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(String path, Set<FileAttribute> attr)
          throws CacheException {
        return request(new PnfsGetFileAttributes(path, attr)).getFileAttributes();
    }

    public FileAttributes getFileAttributes(FsPath path, Set<FileAttribute> attr)
          throws CacheException {
        return getFileAttributes(path.toString(), attr);
    }

    /**
     * Get file attributes. The PnfsManager is free to return less attributes than requested. If
     * <code>attr</code> is an empty array, file existence if checked.
     *
     * @param path
     * @param attr        array of requested attributes.
     * @param mask        Additional AccessMask access rights to check
     * @param updateAtime update file's last access time
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(String path,
          Set<FileAttribute> attr,
          Set<AccessMask> mask, boolean updateAtime)
          throws CacheException {
        PnfsGetFileAttributes msg = new PnfsGetFileAttributes(path, attr);
        msg.setAccessMask(mask);
        msg.setUpdateAtime(updateAtime);
        return request(msg).getFileAttributes();
    }

    /**
     * Set file attributes. If <code>attr</code> is an empty array, file existence if checked.  The
     * updated FileAttribute values in acquire are returned.
     * <p>
     * In principal, the NameSpaceProvider can adjust or ignore updated FileAttribute values.  For
     * Chimera, only updates to ACCESS_TIME and CHANGE_TIME might not be honoured.
     *
     * @param pnfsid
     * @param attr    array of requested attributes.
     * @param acquire set of updated FileAttributes to return.
     * @return The updated values requested via acquire
     */
    public FileAttributes setFileAttributes(PnfsId pnfsid, FileAttributes attr,
          Set<FileAttribute> acquire) throws CacheException {
        return request(new PnfsSetFileAttributes(pnfsid, attr, acquire)).getFileAttributes();
    }

    /**
     * Set file attributes. If <code>attr</code> is an empty array, file existence if checked.
     *
     * @param pnfsid
     * @param attr   array of requested attributes.
     */
    public void setFileAttributes(PnfsId pnfsid, FileAttributes attr) throws CacheException {
        request(new PnfsSetFileAttributes(pnfsid, attr));
    }

    /**
     * Set file attributes by path. If <code>attr</code> is an empty array, file existence if
     * checked.  The updated FileAttribute values requested by the acquire argument are returned.
     * <p>
     * In principal, the NameSpaceProvider can adjust or ignore updated FileAttribute values.  For
     * Chimera, only updates to ACCESS_TIME and CHANGE_TIME might not be honoured.
     *
     * @param path    location of file or directory to modify
     * @param attr    array of requested attributes.
     * @param acquire set of updated FileAttributes to return.
     * @return The updated values requested via acquire
     */
    public FileAttributes setFileAttributes(FsPath path, FileAttributes attr,
          Set<FileAttribute> acquire) throws CacheException {
        return request(
              new PnfsSetFileAttributes(path.toString(), attr, acquire)).getFileAttributes();
    }

    /**
     * Set file attributes by path. If <code>attr</code> is an empty array, file existence if
     * checked.
     *
     * @param path location of file or directory to modify
     * @param attr array of requested attributes.
     */
    public void setFileAttributes(FsPath path, FileAttributes attr) throws CacheException {
        request(new PnfsSetFileAttributes(path.toString(), attr,
              EnumSet.noneOf(FileAttribute.class)));
    }

    /**
     * List all currently existing extended attributes for a file.
     *
     * @param path The file from which all extended attribute are listed.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to list attributes of this
     *                                        file.
     * @throws CacheException                 a generic failure in listing the attributes.
     */
    public Set<String> listExtendedAttributes(FsPath path) throws CacheException {
        return request(new PnfsListExtendedAttributesMessage(path.toString())).getNames();
    }

    /**
     * Obtain the value of an extended attribute.
     *
     * @param path The file from which the extended attribute is read.
     * @param name The ID of the extended attribute.
     * @return The contents of this extended attribute.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to read this attribute.
     * @throws CacheException                 a generic failure in reading the attribute.
     */
    public byte[] readExtendedAttribute(FsPath path, String name)
          throws CacheException {
        PnfsReadExtendedAttributesMessage message =
              new PnfsReadExtendedAttributesMessage(path.toString());
        message.addName(name);
        return request(message).getAllValues().get(name);
    }

    /**
     * Create or modify the value of an extended attribute.
     *
     * @param path  The file for which the extended attribute is created or modified.
     * @param name  The ID of the extended attribute.
     * @param value The value of the attribute if the operation is successful.
     * @param mode  How the attribute value is to be updated.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to modify this attribute.
     * @throws AttributeExistsCacheException  if mode is Mode.CREATE and the attribute exists.
     * @throws NoAttributeCacheException      if mode is Mode.MODIFY and the attribute does not
     *                                        exist.
     * @throws CacheException                 a generic failure in modify the attribute.
     */
    public void writeExtendedAttribute(FsPath path, String name, byte[] value,
          Mode mode) throws CacheException {
        PnfsWriteExtendedAttributesMessage message =
              new PnfsWriteExtendedAttributesMessage(path.toString(), mode);
        message.putValue(name, value);
        request(message);
    }

    /**
     * Create or modify the value of extended attributes.
     *
     * @param path   The file for which the extended attribute is created or modified.
     * @param xattrs The attributes to modify.
     * @param mode   How the attribute value is to be updated.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to modify this attribute.
     * @throws AttributeExistsCacheException  if mode is Mode.CREATE and the attribute exists.
     * @throws NoAttributeCacheException      if mode is Mode.MODIFY and the attribute does not
     *                                        exist.
     * @throws CacheException                 a generic failure in modify the attribute.
     */
    public void writeExtendedAttribute(FsPath path, Map<String, byte[]> xattrs,
          Mode mode) throws CacheException {
        PnfsWriteExtendedAttributesMessage message =
              new PnfsWriteExtendedAttributesMessage(path.toString(), mode);
        xattrs.forEach(message::putValue);
        request(message);
    }

    /**
     * Remove an extended attribute from a file.
     *
     * @param path The file from which the extended attribute is deleted.
     * @param name The name of the extended attribute to remove.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to remove the attribute.
     * @throws NoAttributeCacheException      if the attribute does not exist.
     * @throws CacheException                 a generic failure in removing the attribute.
     */
    public void removeExtendedAttribute(FsPath path, String name)
          throws CacheException {
        PnfsRemoveExtendedAttributesMessage message =
              new PnfsRemoveExtendedAttributesMessage(path.toString());
        message.addName(name);
        request(message);
    }

    /**
     * Remove extended attributes from a file.
     *
     * @param path  The file from which the extended attribute is deleted.
     * @param names The names of the extended attributes to remove.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to remove the attribute.
     * @throws NoAttributeCacheException      if the attribute does not exist.
     * @throws CacheException                 a generic failure in removing the attribute.
     */
    public void removeExtendedAttribute(FsPath path, Collection<String> names)
          throws CacheException {
        PnfsRemoveExtendedAttributesMessage message =
              new PnfsRemoveExtendedAttributesMessage(path.toString());
        names.forEach(message::addName);
        request(message);
    }

    /**
     * Remove a label attribute from a file.
     *
     * @param path  The file from which the label  is deleted.
     * @param label The name of the label to remove.
     * @throws FileNotFoundCacheException     if the path does not exist.
     * @throws PermissionDeniedCacheException if the user is not allowed to remove the label.
     * @throws CacheException                 if the label does not exist or the object is a
     *                                        directory.
     * @throws CacheException                 a generic failure in removing the label.
     */

    public void removeLabel(FsPath path, String label) throws CacheException {

        PnfsRemoveLabelsMessage message =
              new PnfsRemoveLabelsMessage(path.toString());
        message.addLabel(label);
        request(message);
    }
}
