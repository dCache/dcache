// $Id: PnfsHandler.java,v 1.35 2007-10-14 01:51:47 behrmann Exp $

package diskCacheV111.util ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetParentMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import diskCacheV111.vehicles.PnfsSetChecksumMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.acl.enums.AccessMask;
import dmg.cells.nucleus.CellMessageSender;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsCreateSymLinkMessage;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsRemoveChecksumMessage;
import org.dcache.vehicles.PnfsSetFileAttributes;

public class PnfsHandler
    implements CellMessageSender
{
    private final String _poolName;
    private static final long DEFAULT_PNFS_TIMEOUT = TimeUnit.MINUTES.toMillis(30);

    private final CellStub _cellStub;

    private Subject _subject;

    private static final Logger _logNameSpace =
        LoggerFactory.getLogger("logger.org.dcache.namespace." + PnfsHandler.class.getName());

    private static CellStub createStub(CellPath path)
    {
        CellStub stub = new CellStub();
        stub.setDestinationPath(path);
        stub.setTimeout(DEFAULT_PNFS_TIMEOUT);
        stub.setTimeoutUnit(TimeUnit.MILLISECONDS);
        stub.setRetryOnNoRouteToCell(true);
        return stub;
    }

    public PnfsHandler(CellEndpoint endpoint,
                       CellPath pnfsManagerPath)
    {
        this(pnfsManagerPath);
        setCellEndpoint(endpoint);
    }

    public PnfsHandler(CellPath pnfsManagerPath)
    {
        this(createStub(pnfsManagerPath));
    }

    public PnfsHandler(CellPath pnfsManagerPath,
                       String poolName)
    {
        this(createStub(pnfsManagerPath), poolName);
    }

    public PnfsHandler(CellStub stub)
    {
        this(stub, "<client>");
    }

    public PnfsHandler(CellStub stub, String poolName)
    {
        _cellStub = stub;
        _poolName = poolName;
    }

    /**
     * Copy constructor. The primary purpose is to create session
     * specific PnfsHandlers with a session specific subject. Notice
     * that the CellStub is shared between the two handlers and thus
     * the timeout will always be the same.
     *
     * @param handler The PnfsHandler to copy
     * @param subject The Subject to apply to the copy
     */
    public PnfsHandler(PnfsHandler handler, Subject subject)
    {
        _poolName = handler._poolName;
        _cellStub = handler._cellStub;
        _subject = subject;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        _cellStub.setCellEndpoint(endpoint);
    }

    public void setSubject(Subject subject)
    {
        _subject = subject;
    }

    /**
     * Sends a PnfsMessage to PnfsManager.
     *
     * @throws NoRouteToCellException if the PnfsManager could not be reached
     */
    public void send(PnfsMessage msg)
        throws NoRouteToCellException
    {
        if (_cellStub == null) {
            throw new IllegalStateException("Missing endpoint");
        }

        if (_subject != null) {
            msg.setSubject(_subject);
        }

        _cellStub.notify(msg);
    }

    /**
     * Sends a PnfsMessage notification to PnfsManager. No reply is
     * expected for a notification and no failure is reported if the
     * message could not be delivered.
     */
    public void notify(PnfsMessage msg)
    {
        try {
            msg.setReplyRequired(false);
            send(msg);
        } catch (NoRouteToCellException e) {
            _logNameSpace.warn("Failed to deliver message " +
                               msg.getClass().getSimpleName() +
                               " to PnfsManager: " + e.getMessage());
        }
    }

   public void clearCacheLocation(PnfsId id)
   {
       clearCacheLocation(id, _poolName, false);
   }

   public void clearCacheLocation(PnfsId id, boolean removeIfLast)
   {
       clearCacheLocation(id, _poolName, removeIfLast);
   }

   public void clearCacheLocation(PnfsId id, String pool, boolean removeIfLast)
   {
       notify(new PnfsClearCacheLocationMessage(id, pool, removeIfLast));
   }

   public void clearCacheLocation( PnfsId pnfsId , String poolName ){

       notify( new PnfsClearCacheLocationMessage(
                           pnfsId,
                           poolName)
           ) ;

   }

   public void addCacheLocation(PnfsId id) throws CacheException
   {
       addCacheLocation(id, _poolName);
   }

   public void addCacheLocation(PnfsId id, String pool) throws CacheException
   {
       pnfsRequest( new PnfsAddCacheLocationMessage(id, pool));
   }

   public List<String> getCacheLocations( PnfsId pnfsId )throws CacheException {
      PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage(pnfsId) ;
      pnfsMessage = pnfsRequest(pnfsMessage) ;
      List<String> assumedLocations = pnfsMessage.getCacheLocations() ;

      if (assumedLocations == null) {
          return Collections.emptyList();
      } else {
          return assumedLocations;
      }
   }

   public List<String> getCacheLocationsByPath( String fileName )throws CacheException {
      PnfsGetCacheLocationsMessage pnfsMessage = new PnfsGetCacheLocationsMessage() ;
      pnfsMessage.setPnfsPath( fileName ) ;
      pnfsMessage = pnfsRequest(pnfsMessage) ;
      List<String> assumedLocations = pnfsMessage.getCacheLocations() ;

      if (assumedLocations == null) {
          return Collections.emptyList();
      } else {
          return assumedLocations;
      }
   }

    /**
     * Sends a message to the request manager and blocks until a reply
     * is received. In case of errors in the reply, those are thrown
     * as a CacheException. Timeouts and failure to send the message
     * to the PnfsManager are reported as a timeout CacheException.
     */
   public <T extends PnfsMessage> T pnfsRequest( T msg )
           throws CacheException {

       if (_cellStub == null) {
           throw new IllegalStateException("Missing endpoint");
       }

        try {
            msg.setReplyRequired(true);
            if (_subject != null) {
                msg.setSubject(_subject);
            }
            return _cellStub.sendAndWait(msg);
        } catch (InterruptedException e) {
            throw  new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                    "Sending message to PnafsManager intterupted");
        }
   }

    public PnfsCreateEntryMessage createPnfsDirectory(String path)
        throws CacheException
    {
        return pnfsRequest(new PnfsCreateDirectoryMessage(path));
    }

    public PnfsCreateEntryMessage createPnfsDirectory(String path, int uid, int gid, int mode)
        throws CacheException
    {
        return pnfsRequest(new PnfsCreateDirectoryMessage(path, uid, gid, mode));
    }

    /**
     * Creates a directory and all its parent directories.
     *
     * REVISIT: Should eventually be moved to PnfsManager with a flag
     * in the PnfsCreateEntryMessage indicating whether parent
     * directories should be created.
     *
     * @returns the FileAttributes of <code>path</code>
     */
    public FileAttributes createDirectories(FsPath path)
        throws CacheException
    {
        PnfsCreateEntryMessage message;
        try {
            message = createPnfsDirectory(path.toString());
        } catch (FileNotFoundCacheException e) {
            createDirectories(path.getParent());
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

   public PnfsCreateEntryMessage createPnfsEntry( String path )
          throws CacheException                {

       return pnfsRequest(new PnfsCreateEntryMessage( path )) ;

   }

    public PnfsCreateEntryMessage createSymLink(String path, String dest, int uid, int gid)
            throws CacheException {

        return pnfsRequest(new PnfsCreateSymLinkMessage(path, dest, uid, gid));

    }

    public void renameEntry(PnfsId pnfsId, String newName)
        throws CacheException
    {
        renameEntry(pnfsId, newName, true);
    }

    public void renameEntry(PnfsId pnfsId, String newName, boolean overwrite)
        throws CacheException
    {
        pnfsRequest(new PnfsRenameMessage(pnfsId, newName, overwrite));
    }

    public void renameEntry(String path, String newName, boolean overwrite)
        throws CacheException
    {
        pnfsRequest(new PnfsRenameMessage(path, newName, overwrite));
    }

   public PnfsCreateEntryMessage createPnfsEntry( String path , int uid , int gid , int mode )
          throws CacheException                {

       return pnfsRequest( new PnfsCreateEntryMessage( path , uid , gid , mode ) ) ;

   }

    public PnfsId getParentOf(PnfsId pnfsId)
        throws CacheException
    {
            return pnfsRequest(new PnfsGetParentMessage(pnfsId)).getParent();
    }

    public void deletePnfsEntry(String path) throws CacheException
    {
        deletePnfsEntry(null, path);
    }

    public void deletePnfsEntry(String path, Set<FileType> allowed)
        throws CacheException
    {
        deletePnfsEntry(null, path, allowed);
    }

    public void deletePnfsEntry(PnfsId pnfsid)  throws CacheException
    {
        deletePnfsEntry(pnfsid, null);
    }

    public void deletePnfsEntry(PnfsId pnfsid, String path)
        throws CacheException
    {
        deletePnfsEntry(pnfsid, path, EnumSet.allOf(FileType.class));
    }

    public void deletePnfsEntry(PnfsId pnfsid, String path,
                                Set<FileType> allowed)
        throws CacheException
    {
        pnfsRequest(new PnfsDeleteEntryMessage(pnfsid, path, allowed));
    }

   /**
    * Getter for property __pnfsTimeout.
    * @return Value of property __pnfsTimeout.
    */
   public long getPnfsTimeout() {
       return _cellStub.getTimeoutInMillis();
   }

   /**
    * Setter for property __pnfsTimeout.
    * @param __pnfsTimeout New value of property __pnfsTimeout.
    */
   public void setPnfsTimeout(long pnfsTimeout) {
       _cellStub.setTimeout(pnfsTimeout);
       _cellStub.setTimeoutUnit(TimeUnit.MILLISECONDS);
   }

   public String getPnfsFlag(PnfsId pnfsId, String flag)
   throws CacheException
   {
       PnfsFlagMessage flagMessage =
                new PnfsFlagMessage( pnfsId ,flag , PnfsFlagMessage.FlagOperation.GET ) ;
       flagMessage.setReplyRequired( true );

       return pnfsRequest(flagMessage).getValue();
   }

   public void putPnfsFlag(PnfsId pnfsId, String flag, String value)
   {
       PnfsFlagMessage flagMessage =
                new PnfsFlagMessage( pnfsId ,flag , PnfsFlagMessage.FlagOperation.SET ) ;
       flagMessage.setReplyRequired( false );
       flagMessage.setValue(value);
       notify(flagMessage);
   }

   public void fileFlushed(PnfsId pnfsId, FileAttributes fileAttributes) throws CacheException {

	   PoolFileFlushedMessage fileFlushedMessage = new PoolFileFlushedMessage(_poolName, pnfsId, fileAttributes);

	   // throws exception if something goes wrong
	   pnfsRequest(fileFlushedMessage);

   }

	/**
	 * Get path corresponding to given pnfsid.
	 *
	 * @param pnfsID
	 * @return path
	 * @throws CacheException
	 */
	public FsPath getPathByPnfsId(PnfsId pnfsID) throws CacheException {
		return new FsPath(pnfsRequest(new PnfsMapPathMessage(pnfsID)).getPnfsPath());
	}

	/**
	 * Get pnfsid corresponding to given path.
	 *
	 * @param path
	 * @return pnfsid
	 * @throws CacheException
	 */
	public PnfsId getPnfsIdByPath(String path) throws CacheException {
		return pnfsRequest(new PnfsMapPathMessage(path)).getPnfsId();
	}

    /**
     * Get pnfsid corresponding to given path.
     *
     * @param path
     * @param resolve whether sym-links should be followed.  If set to false
     * then the operation will fail if any path element is a symbolic link.
     * @return pnfsid
     * @throws CacheException
     */
    public PnfsId getPnfsIdByPath(String path, boolean resolve)
            throws CacheException {
        PnfsMapPathMessage message = new PnfsMapPathMessage(path);
        message.setShouldResolve(resolve);
        return pnfsRequest(message).getPnfsId();
    }


    /**
     * Remove the registered checksum (of the specified type) from the file
     * with the given id.
     * @param id
     * @param type
     */
    public void removeChecksum(PnfsId id, ChecksumType type)
    {
        notify(new PnfsRemoveChecksumMessage(id, type));
    }

    /**
     * Get file attributes. The PnfsManager is free to return less attributes
     * than requested. If <code>attr</code> is an empty array, file existence
     * if checked.
     *
     * @param pnfsid
     * @param attr array of requested attributes.
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(PnfsId pnfsid, Set<FileAttribute> attr) throws CacheException {
        return pnfsRequest(new PnfsGetFileAttributes(pnfsid, attr)).getFileAttributes();
    }

    /**
     * Get file attributes. The PnfsManager is free to return less attributes
     * than requested. If <code>attr</code> is an empty array, file existence
     * if checked.
     *
     * @param pnfsid
     * @param attr array of requested attributes.
     * @param mask Additional AccessMask access rights to check
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(PnfsId pnfsid, Set<FileAttribute> attr, Set<AccessMask> mask)
        throws CacheException
    {
        PnfsGetFileAttributes msg = new PnfsGetFileAttributes(pnfsid, attr);
        msg.setAccessMask(mask);
        return pnfsRequest(msg).getFileAttributes();
    }

    /**
     * Get file attributes. The PnfsManager is free to return less attributes
     * than requested. If <code>attr</code> is an empty array, file existence
     * if checked.
     *
     * @param path
     * @param attr array of requested attributes.
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(String path, Set<FileAttribute> attr)
        throws CacheException
    {
        return pnfsRequest(new PnfsGetFileAttributes(path, attr)).getFileAttributes();
    }

    public FileAttributes getFileAttributes(FsPath path, Set<FileAttribute> attr)
        throws CacheException
    {
        return getFileAttributes(path.toString(), attr);
    }

    /**
     * Get file attributes. The PnfsManager is free to return less attributes
     * than requested. If <code>attr</code> is an empty array, file existence
     * if checked.
     *
     * @param path
     * @param attr array of requested attributes.
     * @param mask Additional AccessMask access rights to check
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(String path,
                                            Set<FileAttribute> attr,
                                            Set<AccessMask> mask)
        throws CacheException
    {
        PnfsGetFileAttributes msg = new PnfsGetFileAttributes(path, attr);
        msg.setAccessMask(mask);
        return pnfsRequest(msg).getFileAttributes();
    }

    /**
     * Set file attributes. If <code>attr</code> is an empty array,
     * file existence if checked.  The updated FileAttribute values in
     * acquire are returned.
     *
     * In principal, the NameSpaceProvider can adjust or ignore updated
     * FileAttribute values.  For Chimera, only updates to ACCESS_TIME and
     * CHANGE_TIME might not be honoured.
     *
     * @param pnfsid
     * @param attr array of requested attributes.
     * @param acquire set of updated FileAttributes to return.
     * @return The updated values requested via acquire
     */
    public FileAttributes setFileAttributes(PnfsId pnfsid, FileAttributes attr,
            Set<FileAttribute> acquire) throws CacheException
    {
        return pnfsRequest(new PnfsSetFileAttributes(pnfsid, attr, acquire)).getFileAttributes();
    }

    /**
     * Set file attributes. If <code>attr</code> is an empty array,
     * file existence if checked.
     *
     * @param pnfsid
     * @param attr array of requested attributes.
     */
    public void setFileAttributes(PnfsId pnfsid, FileAttributes attr) throws CacheException
    {
        pnfsRequest(new PnfsSetFileAttributes(pnfsid, attr));
    }

    /**
     * Set file attributes by path. If <code>attr</code> is an empty array,
     * file existence if checked.  The updated FileAttribute values requested
     * by the acquire argument are returned.
     *
     * In principal, the NameSpaceProvider can adjust or ignore updated
     * FileAttribute values.  For Chimera, only updates to ACCESS_TIME and
     * CHANGE_TIME might not be honoured.
     *
     * @param path location of file or directory to modify
     * @param attr array of requested attributes.
     * @param acquire set of updated FileAttributes to return.
     * @return The updated values requested via acquire
     */
    public FileAttributes setFileAttributes(FsPath path, FileAttributes attr,
            Set<FileAttribute> acquire) throws CacheException
    {
        return pnfsRequest(new PnfsSetFileAttributes(path.toString(), attr, acquire)).getFileAttributes();
    }

    /**
     * Set file attributes by path. If <code>attr</code> is an empty array,
     * file existence if checked.
     *
     * @param path location of file or directory to modify
     * @param attr array of requested attributes.
     */
    public void setFileAttributes(FsPath path, FileAttributes attr) throws CacheException
    {
        pnfsRequest(new PnfsSetFileAttributes(path.toString(), attr, EnumSet.noneOf(FileAttribute.class)));
    }

    public void setChecksum(PnfsId pnfsId, Checksum checksum)
        throws CacheException
    {
        PnfsSetChecksumMessage message =
            new PnfsSetChecksumMessage(pnfsId,
                                       checksum.getType().getType(),
                                       checksum.getValue());
        pnfsRequest(message);
    }
}
