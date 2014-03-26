/** $Id$
 */


package org.dcache.srm.unixfs;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: FNAL</p>
 * @author  AIK
 * @version 1.0
 */


import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import org.ietf.jgss.GSSCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.ReleaseSpaceCallbacks;
import org.dcache.srm.RemoveFileCallback;
import org.dcache.srm.ReserveSpaceCallbacks;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SrmReleaseSpaceCallback;
import org.dcache.srm.SrmReserveSpaceCallback;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.GridftpClient;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.util.ShellCommandExecuter;
import org.dcache.srm.util.Tools;
import org.dcache.srm.v2_2.TMetaDataSpace;

import static com.google.common.util.concurrent.Futures.immediateCheckedFuture;
import static com.google.common.util.concurrent.Futures.immediateFailedCheckedFuture;

//import java.io.*;
//import java.net.URL;
//import java.net.*;

public class Storage
    implements AbstractStorageElement {

  private static final Logger logger =
          LoggerFactory.getLogger(Storage.class);
  private final static String cvsId = "$Id:";

  private final static String SFN_STRING = "?SFN=";

  private boolean debug = true; // Turns on/off debug messages
  private String gridftphost;
  private int gridftpport;

/** @todo protocols init - this is placehoder*/
  private String[] putProtocols = {"gsiftp","enstore"};
  private String[] getProtocols = {"gsiftp","enstore"};


  private InetAddress myInetAddr;
  private Configuration config; //srm configuration
  private String stat_cmd;
  private String chown_cmd;
  private PrintStream out;
  private PrintStream err;

  public Storage(String gridftphost,
  int gridftpport,
  Configuration configuration,
  String stat_cmd,
  String chown_cmd
  ) {
      this(gridftphost, gridftpport, configuration, stat_cmd, chown_cmd,System.out,System.err);
  }
  /** */
  public Storage(String gridftphost,
  int gridftpport,
  Configuration configuration,
  String stat_cmd,
  String chown_cmd,
  PrintStream out ,
  PrintStream err ) {

      this.config = configuration;
      if(gridftphost == null) {
          throw new IllegalArgumentException("gridftphost is null");
      }
      if(gridftpport <= 0 || gridftpport > 0xFFFF) {
          throw new IllegalArgumentException("illeagal gridftpport="+gridftpport+ "should be in a range [1,0xFFFF]");
      }
      this.gridftphost = gridftphost;
      this.gridftpport = gridftpport;

    try {
      myInetAddr = InetAddress.getByName(gridftphost);
    }
    catch (UnknownHostException ex) {
      myInetAddr = null;
    }
    this.out = out;
    this.err = err;
    StringWriter shell_out = new StringWriter();
    StringWriter shell_err = new StringWriter();
    int return_code = ShellCommandExecuter.execute(stat_cmd+" --help",shell_out, shell_err);
    if(return_code != 0) {
        logger.debug(stat_cmd+" --help output:");
        logger.debug(shell_out.getBuffer().toString());
        logger.error(stat_cmd+" --help error output:");
        logger.error(shell_err.getBuffer().toString());
        logger.error("can not find or run stat command, needed to this Storage Element implementation");
        throw new IllegalArgumentException("stat_cmd=\""+stat_cmd+"\" execution failed with rc = "+return_code);
    }
    this.stat_cmd = stat_cmd;

    shell_out = new StringWriter();
    shell_err = new StringWriter();
    return_code = ShellCommandExecuter.execute(chown_cmd+" --help",shell_out, shell_err);
    if(return_code != 0) {
        logger.debug(chown_cmd+" --help output:");
        logger.debug(shell_out.getBuffer().toString());
        logger.error(chown_cmd+" --help error output:");
        logger.error(shell_err.getBuffer().toString());
        logger.error("can not find or run chown command, needed to this Storage Element implementation");
        throw new IllegalArgumentException("chown_cmd=\""+chown_cmd+"\" execution failed with rc = "+return_code);
    }
    this.chown_cmd = chown_cmd;
  }

  //----------------------------------------------------------------------------
  public static void main(String[] args) {
    //    enstore diskSE = new enstore();
  }

  //----------------------------------------------------------------------------
  // AbstractStorageElement Interface implementation

  //--------------- Sync methods ---

  /** */
  @Override
  public String[] supportedGetProtocols()
  {
    for( String protocol: getProtocols) {
        logger.debug("supportedGetProtocols: " + protocol );
    }

    return getProtocols;
  }

  /** */
  @Override
  public String[] supportedPutProtocols()
  {
    for( String protocol: putProtocols) {
        logger.debug("supportedPutProtocols: " + protocol );
    }
    return putProtocols;
  }

  /** */
  @Override
  public URI getPutTurl(SRMUser user, String localTransferPath, String[] protocols, URI previousTurl)
      throws SRMException {
    /**@todo # Implement getPutTurl() method */

      for (String protocol : protocols) {
          if (protocol.equals("gridftp") || protocol.equals("gsiftp")) {
              return URI
                      .create("gsiftp://" + gridftphost + ":" + gridftpport + "/" + localTransferPath);
          }
          if (protocol.equals("enstore")) {
              return URI
                      .create("enstore://" + gridftphost + ":" + gridftpport + "/" + localTransferPath);
          }
      }
    throw new SRMException("no sutable protocol found");
  }

  /** */
  @Override
  public URI getGetTurl(SRMUser user, URI surl, String[] protocols, URI previousTurl)
      throws SRMException {
    /**@todo # Implement getGetTurl() method */
    String filePath = getPath(surl);
      for (String protocol : protocols) {
          if (protocol.equals("gridftp") || protocol.equals("gsiftp")) {
              return URI
                      .create("gsiftp://" + gridftphost + ":" + gridftpport + "/" + filePath);
          }
          if (protocol.equals("enstore")) {
              return URI
                      .create("enstore://" + gridftphost + ":" + gridftpport + "/" + filePath);
          }

      }
    throw new SRMException("no sutable protocol found");
  }

  /** */
  private void getFromRemoteTURL(SRMUser user, URI remoteTURL, String localTransferPath, SRMUser remoteUser,
    Long remoteCredentialId) throws SRMException {
          if(!(user instanceof UnixfsUser) ){
              throw new SRMException("user is not instance of UnixfsUser");

          }
          UnixfsUser duser = (UnixfsUser)user;

      try
          {
            /**@todo # Implement getFromRemoteTURL() method */
              if(!remoteTURL.getScheme().equalsIgnoreCase("gsiftp") &&
                 !remoteTURL.getScheme().equalsIgnoreCase("gridftp") )
              {
                  throw new SRMException("unsupported protocol : " + remoteTURL.getScheme());
              }
              GSSCredential remoteCredential = RequestCredential.getRequestCredential(remoteCredentialId).getDelegatedCredential();
              GridftpClient client = new GridftpClient(remoteTURL.getHost(),
                                                       remoteTURL.getPort(),
                                                       config.getTcp_buffer_size(),
                remoteCredential);
              client.setStreamsNum(config.getParallel_streams());

              try
              {
		client.gridFTPRead(remoteTURL.getPath(), localTransferPath, true, true);
              }
            finally {
                client.close();
                client = null;
            }
              // now file is copied, we need to change the owner/group to the user's
              changeOwnership(localTransferPath,duser.getUid(),duser.getGid());


          }
          catch(Exception e)
          {
              logger.error(e.toString());
              throw new SRMException("remote turl "+remoteTURL+" to local file "+localTransferPath+" transfer failed",e);
          }


//    SRMException srmEx = new SRMException(
//        "Method getFromRemoteTURL() not yet implemented.");
//    logger.error(srmEx.toString());
//    throw srmEx;
  }

  /** */
  private void putToRemoteTURL(SRMUser user, URI surl, URI remoteTURL, SRMUser remoteUser, Long remoteCredentialId) throws SRMException {

          String path = getPath(surl);
          try
          {
              if(!remoteTURL.getScheme().equalsIgnoreCase("gsiftp") &&
                 !remoteTURL.getScheme().equalsIgnoreCase("gridftp") )
              {
                  throw new SRMException("unsupported protocol : "+remoteTURL.getScheme());
              }
              GSSCredential remoteCredential = RequestCredential.getRequestCredential(remoteCredentialId).getDelegatedCredential();

              GridftpClient client =
                new GridftpClient(remoteTURL.getHost(),
                                  remoteTURL.getPort(),
                                  config.getTcp_buffer_size(),
                                  remoteCredential);
              client.setStreamsNum(config.getParallel_streams());

              try
              {
		client.gridFTPWrite(path,remoteTURL.getPath(),true,true,true);
              }
            finally {
                client.close();
                client = null;
            }


          }
          catch(Exception e)
          {
              logger.error(e.toString());
              throw new SRMException("remote turl "+remoteTURL+" to local file "+surl+" transfer failed",e);
          }
    /**@todo # Implement putToRemoteTURL() method */
//    SRMException srmEx = new SRMException(
//        "Method putToRemoteTURL() not yet implemented.");
//    logger.error(srmEx.toString());
//    throw srmEx;
  }


  private final static String localCopyCommand = "/bin/cp";

  /** */
  @Override
  public void localCopy(SRMUser user,
                        URI fromSurl,
                        String localTransferPath)
      throws SRMException
  {
    /**@todo + localCopy() -- user; check path; set owner & group */

    String[] cmd = new String[3];
    cmd[0] = localCopyCommand;
    cmd[1] = getPath(fromSurl);
    cmd[2] = localTransferPath;

    Process proc;

    try {
      logger.debug("Execute command in the main thread: " +cmd[0]+" "+cmd[1]+" "+cmd[2] );
      proc = Runtime.getRuntime().exec(cmd);
      proc.waitFor();
    }
    catch (IOException ex) {
      logger.error("IOException in localCopy(): " + cmd[0]+" "+cmd[1]+" "+cmd[2] );
      logger.error(ex.toString());
      throw new SRMException( ex );
    }
    catch (InterruptedException ex) {
      logger.error("InterruptedException in localCopy(): " + cmd[0]+" "+cmd[1]+" "+cmd[2] );
      logger.error(ex.toString());
      throw new SRMException( ex );
    }

    int rc = proc.exitValue();
    if( rc != 0 ) {
      // Process Error Return
      SRMException ex = new SRMException("localCopy() filed, rc=" + rc +
            ", command: " +cmd[0]+" "+cmd[1]+" "+cmd[2]);
      throw ex;
    }
    // Succeeded
  }

  /**
   * Checks Transfer URL has the same IP as the Local host.
   */
  @Override
  public boolean isLocalTransferUrl(URI url)
      throws SRMException {
    /**@todo isLocalTransferUrl(): check against multiple hostnames [getAllByName()] */

    if( myInetAddr == null ) {
        throw new SRMException("InetAddress.getLocalHost()," +
                " host name is unknown");
    }

    // use : InetAddress[] getAllByName(String host);
    //
    InetAddress transInetAddr;
    String host = url.getHost();

    try {
      transInetAddr = InetAddress.getByName( host );
    }
    catch (UnknownHostException ex) {
      throw new SRMException("InetAddress.getByName(),"+
                             " Unknown host name for " + host);
    }

    return myInetAddr.equals( transInetAddr ) && this.gridftpport == url.getPort();

  }


  /** */
  //private String _getFileId( FileMetaData fmd )
 // {
 //   return ;
 // }

	@Override
        public void setFileMetaData(SRMUser user, FileMetaData fmd) throws SRMException {
	}
  /**
   * Use File Canonical Path for FileID and FileMetaData.SURL
   * It Is unique, but may be different when file does / does not exist
   *  and it may change when file is created /deleted.
   */
  private FileMetaData _getFileMetaData(SRMUser user, String filePath) throws SRMException {
      StringWriter outWriter = new StringWriter();
      StringWriter errWriter = new StringWriter();
    try {

      File file = new File(filePath);
      if(!file.exists()) {
          throw new IOException("file does not exist");
      }
      String command = stat_cmd+" -t "+file.getCanonicalPath();
      logger.debug("executing command "+command);
      int return_code = ShellCommandExecuter.execute(command,outWriter, errWriter);
      logger.debug("command standard output:"+outWriter.getBuffer().toString());
      if(return_code != 0 ) {
          logger.debug("command error    output:"+errWriter.getBuffer().toString());
          throw new IOException ("command failed with return_code="+return_code);
      }
    } catch (IOException ioe) {
        logger.error(ioe.toString());
        throw new SRMException("can't get the FileMetaData",ioe);
    }
    FileMetaData fmd = new UnixfsFileMetaData(filePath,
        config.getSrmHost(),
        config.getPort(),
        null,
        outWriter.getBuffer().toString());
    return fmd;

/*    boolean exists  = file.exists();
    long    fSize = ( exists && ! file.isDirectory() )
        ? file.length()
        : 0L;

    fmd.SURL = file.getCanonicalPath();
    fmd.isPermanent = new Boolean(exists);
    fmd.size = new Long (fSize);
    if( exists ){
      / ** @todo getFileMetaData(file) - set fmd.owner; group; permMode * /
      fmd.permMode = new Integer(0);
      fmd.owner = "";
      fmd.group = "";
    }
    return fmd;*/
  }

  /** */
  @Override @Nonnull
  public FileMetaData getFileMetaData(SRMUser user, URI surl, boolean read) throws SRMException{

    /**@todo getFileMetaData() - process exception */
    return _getFileMetaData(user, getPath(surl));
  }

    @Nonnull
    @Override
    public FileMetaData getFileMetaData(SRMUser user, URI surl, String fileId) throws SRMException
    {
        return getFileMetaData(user, surl, false);
    }

    /** */
  private File _getFile(String fileId) {
    return new File(fileId);
  }

  /** */
  private File _getFile(String fileId, FileMetaData fmd) {
    logger.debug("_getFile("+fileId);
    return new File(fileId);
  }

  /** */
  private File _getFile(String fileId, FileMetaData fmd,
                        String parentFileId, FileMetaData parentFmd) {
    return new File(fileId);
  }


  //-------------- Async methods ----------------

  /** */
  @Override
  public CheckedFuture<Pin, ? extends SRMException> pinFile(SRMUser user,
                                                            URI surl,
                                                            String clientHost,
                                                            long pinLifetime,
                                                            String requestToken)
  {
    /**@todo - pinFile() - check authorization, do timeout */


    boolean pinned = false;
    String  reason = null;
    String  pinId  = null;
    FileMetaData fmd;

    try {
        fmd = getFileMetaData(user, surl, true);
    } catch (SRMInvalidPathException e) {
        return Futures.immediateFailedCheckedFuture(e);
    } catch (SRMException ex) {
        logger.error(ex.toString());
        return Futures.immediateFailedCheckedFuture(ex);
    }


    if (fmd.isDirectory) {
        return Futures.immediateFailedCheckedFuture(new SRMInvalidPathException("Path is a directory"));
    }

    try {
      File file = _getFile(fmd.fileId, fmd);
      pinned = file.exists();
      logger.debug("file exists is "+pinned);
      if( pinned )

      {
          pinId = fmd.fileId;
      } else {
          reason = "file does not exist";
      }
    }
    catch (Exception ex) {
      reason = "got exception " + ex;
      logger.error(ex.toString());
    }

   // Return filedId as PinId
   if( pinned ) {
       return Futures.immediateCheckedFuture(new Pin(fmd, pinId));
   } else {
       return Futures.immediateFailedCheckedFuture(new SRMException(reason));
   }
  }

  /** */
  @Override
  public CheckedFuture<String, ? extends SRMException> unPinFile(SRMUser user, String fileId, String pinId)
  {
    boolean unpinned = false;
    String  reason   = null;

    try {
      File file = _getFile(fileId);
      unpinned = file.exists();
      if( unpinned ) {
          pinId = fileId;
      } else {
          reason = "file does not exist";
      }
    }
    catch (Exception ex) {
      reason = "got exception " + ex;
      logger.error(ex.toString());
    }

   if( unpinned ) {
       return Futures.immediateCheckedFuture(pinId);
   } else {
       return Futures.immediateFailedCheckedFuture(new SRMException(reason));
   }
  }

  /** */
  @Override
  public void advisoryDelete(SRMUser user, URI surl,
                             AdvisoryDeleteCallbacks callbacks) {
    /**@todo advisoryDelete() - user, async */
    if( ! ( callbacks instanceof AdvisoryDeleteCallbacks ) ) {
        throw new IllegalArgumentException(
                "Method advisoryDelete() has wrong callback argument type.");
    }

    boolean deleted = false;
    String  reason  = null;

    try {
      /**@todo advisoryDelete() - _getFile(filePath) assumes fileID == filePath */
      // fileId is Canonical path
      File file = _getFile(getPath(surl));
      deleted   = file.delete();
      if( ! deleted ) {
          reason = "delete file operation failed";
      }
    }
    catch (Exception ex) {
      reason = "got exception " + ex;
      logger.error(ex.toString());
    }

    if( deleted ) {
        callbacks.AdvisoryDeleteSuccesseded();
    } else {
        callbacks.AdvisoryDeleteFailed(reason);
    }

  }

  /**
   * _installPath checks that
   * - path is directory path and it exists
   * If it does not exist it checks parent directories recurcevely
   *   and creates missing path elements starting from existing elemant in the path
   *   down to this "path"
   * */

  private boolean _installPath(SRMUser user, String path) {
    logger.debug("_installPath("+user+","+path+")");
    File file   = new File(path);

    if ( file.exists() ) {
    logger.debug("_installPath: file exists, returning "+(file.isDirectory() && file.canWrite() ));
      return ( file.isDirectory() && file.canWrite() );
    }

    // If this dir does not exist,
    //   check / create leading path elements
    String pPath = file.getParent();
    if( pPath == null ) {
        return false;
    }

    if( ! _installPath(user, pPath ) ) {
        return false;
    }

    //   and create directory itself
    return file.mkdir();
  }

  @Override
  public CheckedFuture<String, ? extends SRMException> prepareToPut(SRMUser user, URI surl,
                                                                    Long size,
                                                                    String accessLatency,
                                                                    String retentionPolicy,
                                                                    String spaceToken,
                                                                    boolean overwrite)
  {
      String filePath;
      try {
          filePath = getPath(surl);
      } catch (SRMInvalidPathException e) {
          return immediateFailedCheckedFuture(e);
      }

      File file = new File(filePath);
      if (file.exists()) {
          if (!overwrite) {
              return immediateFailedCheckedFuture(new SRMDuplicationException("file exists, can't overwrite."));
          }
      } else {
          if (!_installPath(user, file.getParent()) ) {
              return immediateFailedCheckedFuture(new SRMInternalErrorException(
                      "prepareToPut() can not get or create parent for the filePath=" + filePath + "."));
          }
      }

      logger.debug( "prepareToPut(): {} ", filePath);
      return immediateCheckedFuture(filePath);
  }

    @Override
    public void putDone(SRMUser user, String localTransferPath, URI surl, boolean overwrite)
    {
        // Nothing to do
    }

    @Override
    public void abortPut(SRMUser user, String localTransferPath, URI surl, String reason)
            throws SRMInternalErrorException
    {
        try {
            Files.deleteIfExists(new File(localTransferPath).toPath());
        } catch (IOException e) {
            throw new SRMInternalErrorException(e.getMessage(), e);
        }
    }

    /**
   * Not implemented.<br>
   * This is a feature of SRM interface v2.0
   * */
  public void reserveSpace(SRMUser user, long spaceSize, long reservationLifetime, ReserveSpaceCallbacks callbacks) {
      callbacks.SpaceReserved("dummy", spaceSize);
  }

  /**
   * Not implemented.<br>
   * This is a feature of SRM interface v2.0
   * */
  public void releaseSpace(SRMUser user, String spaceToken, ReleaseSpaceCallbacks callbacks) {
      callbacks.SpaceReleased();
  }
  /**
   * Not implemented.<br>
   * This is a feature of SRM interface v2.0
   * */
  public void releaseSpace(SRMUser user, long spaceSize,String spaceToken, ReleaseSpaceCallbacks callbacks) {
    callbacks.SpaceReleased();
  }


  private void changeOwnership(String filePath, int uid, int gid) throws IOException {
      StringWriter outWriter = new StringWriter();
      StringWriter errWriter = new StringWriter();

      File file = new File(filePath);
      if(!file.exists()) {
          throw new IOException("file does not exist");
      }
      String command = chown_cmd+" "+uid+"."+gid+" "+file.getCanonicalPath();
      logger.debug("executing command "+command);
      int return_code = ShellCommandExecuter.execute(command,outWriter, errWriter);
      logger.debug("command standard output:"+outWriter.getBuffer().toString());
      if(return_code != 0 ) {
          logger.debug("command error    output:"+errWriter.getBuffer().toString());
          throw new IOException ("command failed with return_code="+return_code);
      }

  }

  private static long unique_id;
  private static synchronized String getUniqueId() {
      return Long.toHexString(unique_id++);

  }
  private Map<String, Thread> copyThreads = new HashMap<>();

  /**
     *
   * @param user User ID
   * @param remoteTURL
   * @param localTransferPath
   * @param remoteUser
   * @param callbacks
   * @throws SRMException
     * @return transfer id
     */
    @Override
    public String getFromRemoteTURL(
        final SRMUser user,
        final URI remoteTURL,
        final String localTransferPath,
        final SRMUser remoteUser,
        final Long remoteCredentialId,
        final CopyCallbacks callbacks) throws SRMException{
        Thread t = new Thread(){

            @Override
            public void run() {
                try
                {
                    logger.debug("calling getFromRemoteTURL from a copy thread");
                    getFromRemoteTURL(user, remoteTURL, localTransferPath, remoteUser, remoteCredentialId);
                    logger.debug("calling callbacks.copyComplete for path="+ localTransferPath);
                    callbacks.copyComplete();
                }
                catch (Exception e){
                    callbacks.copyFailed(new SRMException(e));
                }
            }
        };
        String id = getUniqueId();
        logger.debug("getFromRemoteTURL assigned id ="+id+"for transfer from "+remoteTURL+" to "+ localTransferPath);

        copyThreads.put(id, t);
        t.start();
        return id;
    }
    /**
     * @param user User ID
     * @param actualFilePath
     * @param remoteTURL
     * @param remoteUser
     * @param callbacks
     * @param remoteCredetial
     * @throws SRMException
     * @return transfer id
     */
    @Override
    public String putToRemoteTURL(final SRMUser user, final URI surl,final URI remoteTURL, final SRMUser remoteUser, final Long remoteCredentialId, final CopyCallbacks callbacks)
    throws SRMException {

       Thread t = new Thread(){

           @Override
            public void run() {
                try
                {
                    logger.debug("calling putToRemoteTURL from a copy thread");
                    putToRemoteTURL(user, surl, remoteTURL, remoteUser, remoteCredentialId);
                    logger.debug("calling callbacks.copyComplete for path="+surl);
                    callbacks.copyComplete();
                }
                catch (Exception e){
                    callbacks.copyFailed(new SRMException(e));
                }
            }
        };
        String id = getUniqueId();
        logger.debug("putToRemoteTURL assigned id ="+id+"for transfer from "+surl+" to "+remoteTURL);

        copyThreads.put(id, t);
        t.start();
        return id;
    }


  @Override
  public void killRemoteTransfer(String transferId) {
      Thread t = copyThreads.get(transferId);
      if(t == null) {
          logger.debug("killRemoteTransfer: cannot find thread for transfer with id="+ transferId);
      }
      else
      {
          logger.debug("killRemoteTransfer: found thread for transfer with id="+ transferId+", killing");
          t.interrupt();
      }
  }

  public void reserveSpace(SRMUser user, long spaceSize, long reservationLifetime, String filename, String host, ReserveSpaceCallbacks callbacks) {
      callbacks.SpaceReserved("dummy", spaceSize);
  }

      @Override
      public void removeFile(final SRMUser user,
			     final URI path,
			     RemoveFileCallback callbacks) {
      }

      @Override
      public void removeDirectory(SRMUser user,
				  URI surl,
                                  boolean recursive)  throws SRMException {
      }

      @Override
      public void createDirectory(final SRMUser user,
				  final URI directory) throws SRMException {
      }

      @Override
      public List<URI> listDirectory(SRMUser user, URI surl, FileMetaData fileMetaData) throws SRMException {
          String directoryName = getPath(surl);
          FileMetaData fmd = _getFileMetaData(user, directoryName);
         int uid = Integer.parseInt(fmd.owner);
         int gid = Integer.parseInt(fmd.group);
         int permissions = fmd.permMode;

         if(permissions == 0 ) {
            throw new SRMException ("permission denied");
         }

         if(Permissions.worldCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }

         if(uid == -1 || gid == -1) {
            throw new SRMException ("permission denied");
         }

         if(user == null ) {
            throw new SRMException ("permission denied");
         }

         if(fmd.isGroupMember(user) && Permissions.groupCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }

         if(fmd.isOwner(user) && Permissions.userCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }
         File f = new File(directoryName);
         if(!f.isDirectory()) {
             throw new SRMException("not a directory");
         }
         String base = addTrailingSlash(surl.toString());
         List<URI> result = new ArrayList<>();
         for (String file: f.list()) {
             result.add(URI.create(base + file));
         }
         return result;
      }

    @Override
    public List<FileMetaData>
        listDirectory(SRMUser user, URI surl, final boolean verbose,
                      int offset, int count)
        throws SRMException
    {
        List<URI> list = listDirectory(user, surl, null);
        List<FileMetaData> result = new ArrayList<>();
        for (int i = offset; i < list.size() && i < offset + count; i++) {
            result.add(getFileMetaData(user, list.get(i), false));
        }
        return result;
    }

        @Override
	public void moveEntry(SRMUser user, URI from, URI to)
          throws SRMException
        {
	}

    @Override
    public void srmReserveSpace(SRMUser user, long sizeInBytes, long spaceReservationLifetime, String retentionPolicy, String accessLatency, String description,SrmReserveSpaceCallback callbacks) {
    }

    @Override
    public void srmReleaseSpace(SRMUser user, String spaceToken, Long sizeInBytes, SrmReleaseSpaceCallback callbacks) {
    }

    /**
     *
     * @param spaceTokens
     * @throws SRMException
     * @return
     */
    @Override
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user, String[] spaceTokens)
        throws SRMException {
        return null;
    }
    /**
     *
     * @param description
     * @throws SRMException
     * @return
     */
    @Override @Nonnull
    public String[] srmGetSpaceTokens(SRMUser user,String description)
        throws SRMException {
        return new String[0];
    }

    /**
     *
     *
     *
     * @param newLifetime SURL lifetime in seconds
     *   -1 stands for infinite lifetime
     * @return long lifetime left in seconds
     *   -1 stands for infinite lifetime
     */
    @Override
    public long srmExtendSurlLifetime(SRMUser user, URI surl, long newLifetime) throws SRMException {
        FileMetaData fmd = getFileMetaData(user,surl, true);
        int uid = Integer.parseInt(fmd.owner);
        int gid = Integer.parseInt(fmd.group);
        int permissions = fmd.permMode;

        if(Permissions.worldCanWrite(permissions)) {
            return -1;
        }

        if(uid == -1 || gid == -1) {
            throw new SRMAuthorizationException("User is not authorized to modify this file");
        }

        if(user == null || (!(user instanceof UnixfsUser))) {
            throw new SRMAuthorizationException("User is not authorized to modify this file");
        }
        UnixfsUser duser = (UnixfsUser) user;

        if(duser.getGid() == gid && Permissions.groupCanWrite(permissions)) {
            return -1;
        }

        if(duser.getUid() == uid && Permissions.userCanWrite(permissions)) {
            return -1;
        }

        throw new SRMAuthorizationException("User is not authorized to modify this file");

    }

    @Override
    public long extendPinLifetime(SRMUser user, String fileId, String pinId, long newPinLifetime) throws SRMException {
        return newPinLifetime;
    }

    @Override
    public long srmExtendReservationLifetime(SRMUser user, String spaceToken, long newReservationLifetime) throws SRMException {
        return newReservationLifetime;
    }

    @Override
    public String getStorageBackendVersion() { return "$Revision: 1.35 $"; }

    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFileBySrmRequestId(
            SRMUser user, String fileId, String requestToken)
    {
        return Futures.immediateCheckedFuture(fileId);
    }

   /** This method allows to unpin file in the Storage Element,
     * i.e. cancel the requests to have the file in "fast access state"
     * This method will remove all pins on this file user has permission
     * to remove
    * @param user User ID
    * @param fileId Storage Element internal file ID
    */
    @Override
    public CheckedFuture<String, ? extends SRMException> unPinFile(SRMUser user, String fileId)
    {
        return Futures.immediateCheckedFuture(fileId);
    }

    /**
     * Adds a trailing slash to a string unless the string already has
     * a trailing slash.
     */
    private String addTrailingSlash(String s)
    {
        if (!s.endsWith("/")) {
            s = s + "/";
        }
        return s;
    }

    /**
     * Given a path relative to the root path, this method returns a
     * full PNFS path.
     */
    private String getPath(URI surl)
        throws SRMInvalidPathException
    {
        try {
            String path = surl.getPath();
            String scheme = surl.getScheme();
            if (scheme != null) {
                if (!scheme.equalsIgnoreCase("srm")) {
                    throw new SRMInvalidPathException("Invalid scheme: " + scheme);
                }
                if (!Tools.sameHost(config.getSrmHosts(), surl.getHost())) {
                    throw new SRMInvalidPathException("SURL is not local: " + surl);
                }

                int i = path.indexOf(SFN_STRING);
                if (i != -1) {
                    path = path.substring(i + SFN_STRING.length());
                }
            }
            return path;
        } catch (UnknownHostException e) {
            throw new SRMInvalidPathException(e.getMessage());
        }
    }

}
