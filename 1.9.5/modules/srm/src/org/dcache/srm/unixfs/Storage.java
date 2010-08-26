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


import org.dcache.srm.AbstractStorageElement;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;
import diskCacheV111.srm.StorageElementInfo;

import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.GetFileInfoCallbacks;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.PrepareToPutInSpaceCallbacks;
import org.dcache.srm.PinCallbacks;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.UnpinCallbacks;
import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.ReserveSpaceCallbacks;
import org.dcache.srm.ReleaseSpaceCallbacks;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.SRMUser;
import org.dcache.srm.util.GridftpClient;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.v2_2.TMetaDataSpace;
import org.ietf.jgss.GSSCredential;
//import java.io.*;
import java.io.File;
import java.io.IOException;

import java.net.InetAddress;
//import java.net.URL;
import org.globus.util.GlobusURL;
//import java.net.*;
import java.net.UnknownHostException;
import java.net.MalformedURLException;
import org.dcache.srm.util.ShellCommandExecuter;

import java.io.StringWriter;
import java.io.PrintStream;

import org.dcache.srm.RemoveFileCallbacks;

import java.util.*;

public class Storage
    implements AbstractStorageElement {

  private final static String cvsId = "$Id:";

  private boolean debug = true; // Turns on/off debug messages
  private String gridftphost;
  private int gridftpport;

/** @todo protocols init - this is placehoder*/
  private String[] putProtocols = {"gsiftp","enstore"};
  private String[] getProtocols = {"gsiftp","enstore"};


  private InetAddress myInetAddr = null;
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
        log(stat_cmd+" --help output:");
        log(shell_out.getBuffer().toString());
        elog(stat_cmd+" --help error output:");
        elog(shell_err.getBuffer().toString());
        elog("can not find or run stat command, needed to this Storage Element implementation");
        throw new IllegalArgumentException("stat_cmd=\""+stat_cmd+"\" execution failed with rc = "+return_code);
    }
    this.stat_cmd = stat_cmd;
    
    shell_out = new StringWriter();
    shell_err = new StringWriter();
    return_code = ShellCommandExecuter.execute(chown_cmd+" --help",shell_out, shell_err);
    if(return_code != 0) {
        log(chown_cmd+" --help output:");
        log(shell_out.getBuffer().toString());
        elog(chown_cmd+" --help error output:");
        elog(shell_err.getBuffer().toString());
        elog("can not find or run chown command, needed to this Storage Element implementation");
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
  public String[] supportedGetProtocols()
      throws SRMException {
      
    for( String protocol: getProtocols) {
        dlog("supportedGetProtocols: " + protocol );
    }

    return getProtocols;
  }

  /** */
  public String[] supportedPutProtocols()
      throws SRMException {

    for( String protocol: putProtocols) {
        dlog("supportedPutProtocols: " + protocol );
    }

    return putProtocols;
  }

  /** */
  public String getPutTurl(SRMUser user, String filePath, String[] protocols)
      throws SRMException {
    /**@todo # Implement getPutTurl() method */

    for(int i= 0; i<protocols.length; ++i) {
        if(protocols[i].equals("gridftp") || protocols[i].equals("gsiftp") ) {
            return "gsiftp://"+gridftphost+":"+gridftpport+"/"+filePath;
        }
        if(protocols[i].equals("enstore")) {
            return "enstore://"+gridftphost+":"+gridftpport+"/"+filePath;
        }
    }
    throw new SRMException("no sutable protocol found");
    //SRMException srmEx = new SRMException(
    //    "Method getPutTurl() not yet implemented.");
    //elog( srmEx );
    //throw srmEx;
  }

  /**
   * Not implemented.<br>
   * This is a feature of dCache implementation of SRM SE for DCAP protocol.
   * */

  public String getPutTurl(SRMUser user, String filePath, String previous_turl)
      throws SRMException {
    return  getPutTurl(user,filePath,new String[]{previous_turl});
    /*
    String erStr1 = "Method getPutTurl(..., previous_turl) not implemented.";
    String erStr2 = "This method is a feature of implementation dCache SRM SE and "+
        "it must not be called for the disk SE\n";

    elog( erStr1 + "\n" + erStr2 );

    SRMException srmEx = new SRMException( erStr1 );
    throw srmEx;
     */
  }

  /** */
  public String getGetTurl(SRMUser user, String filePath, String[] protocols)
      throws SRMException {
    /**@todo # Implement getGetTurl() method */
    for(int i= 0; i<protocols.length; ++i) {
        if(protocols[i].equals("gridftp") || protocols[i].equals("gsiftp") ) {
	    return "gsiftp://"+gridftphost+":"+gridftpport+"/"+filePath;
        }
        if(protocols[i].equals("enstore")) {
            return "enstore://"+gridftphost+":"+gridftpport+"/"+filePath;
        }
	
    }
    throw new SRMException("no sutable protocol found");
    /*SRMException srmEx = new SRMException(
        "Method getGetTurl() not yet implemented.");
    elog(srmEx);
    throw srmEx;*/
  }

  /**
   * Not implemented.<br>
   * This is a feature of dCache implementation of SRM SE for DCAP protocol.
   * */

  public String getGetTurl(SRMUser user, String filePath, String previous_turl)
      throws SRMException {
    return  getPutTurl(user,filePath,new String[]{previous_turl});
/*
    String erStr1 = "Method getGetTurl(..., previous_turl) not implemented.";
    String erStr2 = "This method is a feature of implementation dCache SRM SE and "+
        "it must not be called for the disk SE\n";
    elog( erStr1 + "\n" + erStr2 );

    SRMException srmEx = new SRMException( erStr1 );
    throw srmEx;
 */
  }

  /** */
  public void getFromRemoteTURL(SRMUser user, String remoteTURL, String actualFilePath, SRMUser remoteUser,
    Long remoteCredentialId) throws SRMException {
          if(!(user instanceof UnixfsUser) ){
              throw new SRMException("user is not instance of UnixfsUser");
              
          }
          UnixfsUser duser = (UnixfsUser)user;
          
          try
          {
            /**@todo # Implement getFromRemoteTURL() method */
              GlobusURL url = new GlobusURL(remoteTURL);
              if(!url.getProtocol().equalsIgnoreCase("gsiftp") && 
                 !url.getProtocol().equalsIgnoreCase("gridftp") )
              {
                  throw new SRMException("unsupported protocol : "+url.getProtocol());
              }
              GSSCredential remoteCredential = RequestCredential.getRequestCredential(remoteCredentialId).getDelegatedCredential();
              GridftpClient client = new GridftpClient(url.getHost(),
                url.getPort(), config.getTcp_buffer_size(),
                remoteCredential,this);
              client.setStreamsNum(config.getParallel_streams());
    	      
              try
              {
		client.gridFTPRead(url.getPath(),actualFilePath , true,true);
              }
            finally {
                client.close();
                client = null;
            }
              // now file is copied, we need to change the owner/group to the user's 
              changeOwnership(actualFilePath,duser.getUid(),duser.getGid());


          }
          catch(Exception e)
          {
              elog(e);
              throw new SRMException("remote turl "+remoteTURL+" to local file "+actualFilePath+" transfer failed",e);
          }
          
          
//    SRMException srmEx = new SRMException(
//        "Method getFromRemoteTURL() not yet implemented.");
//    elog(srmEx);
//    throw srmEx;
  }

  /** */
  public void putToRemoteTURL(SRMUser user, String actualFilePath, String remoteTURL, SRMUser remoteUser, Long remoteCredentialId) throws SRMException {
          try
          {
              GlobusURL url = new GlobusURL(remoteTURL);
              if(!url.getProtocol().equalsIgnoreCase("gsiftp") && 
                 !url.getProtocol().equalsIgnoreCase("gridftp") )
              {
                  throw new SRMException("unsupported protocol : "+url.getProtocol());
              }
              GSSCredential remoteCredential = RequestCredential.getRequestCredential(remoteCredentialId).getDelegatedCredential();

              GridftpClient client = new GridftpClient(url.getHost(),
                url.getPort(), config.getTcp_buffer_size(),
                remoteCredential,this);
              client.setStreamsNum(config.getParallel_streams());

              try
              {
		client.gridFTPWrite(actualFilePath,url.getPath(),true,true,true);
              }
            finally {
                client.close();
                client = null;
            }


          }
          catch(Exception e)
          {
              elog(e);
              throw new SRMException("remote turl "+remoteTURL+" to local file "+actualFilePath+" transfer failed",e);
          }
    /**@todo # Implement putToRemoteTURL() method */
//    SRMException srmEx = new SRMException(
//        "Method putToRemoteTURL() not yet implemented.");
//    elog(srmEx);
//    throw srmEx;
  }


  private final static String localCopyCommand = "/bin/cp";

  /** */
  public void localCopy(SRMUser user, String actualFromFilePath,
                        String actualToFilePath)
      throws SRMException
  {
    /**@todo + localCopy() -- user; check path; set owner & group */

    String[] cmd = new String[3];
    cmd[0] = localCopyCommand;
    cmd[1] = actualFromFilePath;
    cmd[2] = actualToFilePath;

    Process proc;

    try {
      dlog("Execute command in the main thread: " +cmd[0]+" "+cmd[1]+" "+cmd[2] );
      proc = Runtime.getRuntime().exec(cmd);
      proc.waitFor();
    }
    catch (IOException ex) {
      elog("IOException in localCopy(): " + cmd[0]+" "+cmd[1]+" "+cmd[2] );
      elog(ex);
      throw new SRMException( ex );
    }
    catch (InterruptedException ex) {
      elog("InterruptedException in localCopy(): " + cmd[0]+" "+cmd[1]+" "+cmd[2] );
      elog(ex);
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
  public boolean isLocalTransferUrl(String urlArg)
      throws SRMException {
    /**@todo isLocalTransferUrl(): check against multiple hostnames [getAllByName()] */

    GlobusURL url = null;
    try {
      url = new GlobusURL(urlArg);
    }
    catch (MalformedURLException ex) {
      throw new SRMException("Malformed URL argument to isLocalTransferUrl(),"+
                       " URL=" + urlArg );
    }


    if( myInetAddr == null )
      throw new SRMException("InetAddress.getLocalHost(),"+
                             " host name is unknown");

    // use : InetAddress[] getAllByName(String host);
    //
    InetAddress transInetAddr;
    String host = url.getHost();

    try {
      transInetAddr = InetAddress.getByName( host );
    }
    catch (UnknownHostException ex) {
      transInetAddr = null;
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

	public void setFileMetaData(SRMUser user, FileMetaData fmd) throws SRMException {
	}
  /**
   * Use File Canonical Path for FileID and FileMetaData.SURL
   * It Is unique, but may be different when file does / does not exist
   *  and it may change when file is created /deleted.
   */
  private FileMetaData _getFileMetaData(SRMUser user, String filePath) throws SRMException {
      StringWriter out = new StringWriter();
      StringWriter err = new StringWriter();
    try {
       
      File file = new File(filePath);
      if(!file.exists()) throw new IOException("file does not exist");
      String command = stat_cmd+" -t "+file.getCanonicalPath();
      log("executing command "+command);
      int return_code = ShellCommandExecuter.execute(command,out, err);
      log("command standard output:"+out.getBuffer().toString());
      if(return_code != 0 ) {
          log("command error    output:"+err.getBuffer().toString());
          throw new IOException ("command failed with return_code="+return_code);
      }
    } catch (IOException ioe) {
        elog(ioe);
        throw new SRMException("can't get the FileMetaData",ioe);
    }
    FileMetaData fmd = new UnixfsFileMetaData(filePath,config.getSrmhost(),
        config.getPort(),
        null,
        out.getBuffer().toString());
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
  public FileMetaData getFileMetaData(SRMUser user, String filePath) throws SRMException{

    /**@todo getFileMetaData() - process exception */
    return _getFileMetaData(user, filePath);
  }

  /** */
  private File _getFile(String fileId) {
    return new File(fileId);
  }

  /** */
  private File _getFile(String fileId, FileMetaData fmd) {
    log("_getFile("+fileId);
    return new File(fileId);
  }

  /** */
  private File _getFile(String fileId, FileMetaData fmd,
                        String parentFileId, FileMetaData parentFmd) {
    return new File(fileId);
  }

        public boolean canRead(SRMUser user, String fileId, FileMetaData fmd) {
            return _canRead(user,fileId,fmd);
        }
        
        public static boolean _canRead(SRMUser user, String fileId, FileMetaData fmd) {
            int uid = Integer.parseInt(fmd.owner);
            int gid = Integer.parseInt(fmd.group);
            int permissions = fmd.permMode;
            
            
            if(permissions == 0 ) {
                return false;
            }
            
            if(Permissions.worldCanRead(permissions)) {
                return true;
            }
            
            if(uid == -1 || gid == -1) {
                return false;
            }
            
            if(user == null || (!(user instanceof UnixfsUser))) {
                return false;
            }
            UnixfsUser duser = (UnixfsUser) user;
            
            if(duser.getGid() == gid && Permissions.groupCanRead(permissions)) {
                return true;
            }
            
            if(duser.getUid() == uid && Permissions.userCanRead(permissions)) {
                return true;
            }
            
            return false;
            
            
        }
        
        public boolean canWrite(SRMUser user, String fileId, FileMetaData fmd, 
                String parentFileId, FileMetaData parentFmd, boolean overwrite) {
            return _canWrite(user,fileId,fmd,parentFileId,parentFmd,overwrite);
        }
        
        public static boolean _canWrite(SRMUser user, String fileId,
                FileMetaData fmd,String parentFileId, FileMetaData parentFmd,
                boolean overwrite) {
            // we can not overwrite file in dcache (at least for now)
            if(fileId != null ) {
                return false;
            }
            
            if( parentFileId == null) {
                return false;
            }
            int uid = Integer.parseInt(parentFmd.owner);
            int gid = Integer.parseInt(parentFmd.group);
            int permissions = parentFmd.permMode;
            
            if(permissions == 0 ) {
                return false;
            }
            
            if(Permissions.worldCanWrite(permissions) &&
            Permissions.worldCanExecute(permissions)) {
                return true;
            }
            
            if(uid == -1 || gid == -1) {
                return false;
            }
            
            if(user == null || (!(user instanceof UnixfsUser))) {
                return false;
            }
            UnixfsUser duser = (UnixfsUser) user;
            
            
            if(duser.getGid() == gid &&
            Permissions.groupCanWrite(permissions) &&
            Permissions.groupCanExecute(permissions)) {
                return true;
            }
            
            if(duser.getUid() == uid &&
            Permissions.userCanWrite(permissions) &&
            Permissions.userCanExecute(permissions)) {
                return true;
            }
            
            return false;
            
            
        }
        
        public static boolean _canDelete(SRMUser user, String fileId,FileMetaData fmd) {
            // we can not overwrite file in dcache (at least for now)
            if(fileId == null ) {
                return false;
            }
            
            int uid = Integer.parseInt(fmd.owner);
            int gid = Integer.parseInt(fmd.group);
            int permissions = fmd.permMode;
            
            if(permissions == 0 ) {
                return false;
            }
            
            if(Permissions.worldCanWrite(permissions) &&
            Permissions.worldCanExecute(permissions)) {
                return true;
            }
            
            if(uid == -1 || gid == -1) {
                return false;
            }
            
            if(user == null || (!(user instanceof UnixfsUser))) {
                return false;
            }
            UnixfsUser duser = (UnixfsUser) user;
            
            
            if(duser.getGid() == gid &&
            Permissions.groupCanWrite(permissions) &&
            Permissions.groupCanExecute(permissions)) {
                return true;
            }
            
            if(duser.getUid() == uid &&
            Permissions.userCanWrite(permissions) &&
            Permissions.userCanExecute(permissions)) {
                return true;
            }
            
            return false;
            
            
        }

  /** */
  public StorageElementInfo getStorageElementInfo(SRMUser user)
      throws SRMException {
    /**@todo # Implement getStorageElementInfo() method */
    SRMException srmEx = new SRMException(
        "Method getStorageElementInfo() not yet implemented.");
    elog( srmEx );
    throw srmEx;
  }

  //-------------- Async methods ----------------

  /** */
  public void getFileInfo(SRMUser user, String filePath,
                          GetFileInfoCallbacks callbacks) {
    /**@todo getFileInfo() - async, lost fileId */

    if( ! ( callbacks instanceof GetFileInfoCallbacks ) )
      throw new java.lang.IllegalArgumentException(
          "Method getFileInfo() has wrong callback argument type.");

    FileMetaData fmd    = null;
    String       fileId = null;

    try {
      fmd    = _getFileMetaData( user, filePath );
      fileId = filePath;
    }
    catch (Exception ex) {
      elog( ex );

      String erStr = "getFileInfo() got exception for the filePath=" +filePath +".";
      callbacks.GetStorageInfoFailed( erStr );
      return;
    }

    dlog( "getFileInfo(): StorageInfoArrived, fileId="+fileId + "fmd=" + fmd );
    callbacks.StorageInfoArrived(fileId, fmd);
  }


  /** */
  public void pinFile(SRMUser user,
          String fileId, 
          String clientHost,
          FileMetaData fmd, 
          long pinLifetime, 
          long requestId,
          PinCallbacks callbacks) {
    /**@todo - pinFile() - check authorization, do timeout */

    if( ! (callbacks instanceof PinCallbacks ) )
      throw new java.lang.IllegalArgumentException(
          "Method pinFile() has wrong callback argument type.");

    boolean pinned = false;
    String  reason = null;
    String  pinId  = null;

    try {
      File file = _getFile(fileId, fmd);
      pinned = file.exists();
      log("file exists is "+pinned);
      if( pinned )
        
        pinId = fileId;
      else
        reason = "file does not exist";
    }
    catch (Exception ex) {
      reason = "got exception " + ex;
      elog( ex );
    }

   // Return filedId as PinId
   if( pinned )
     callbacks.Pinned( pinId );
   else
     callbacks.PinningFailed( reason );
  }

  /** */
  public void unPinFile(SRMUser user, String fileId,
                        UnpinCallbacks callbacks, String pinId) {
  // Ignore pinId argument internally for now, use it for return only

    if( ! ( callbacks instanceof UnpinCallbacks ) )
      throw new java.lang.IllegalArgumentException(
          "Method unPinFile() has wrong callback argument type.");

    boolean unpinned = false;
    String  reason   = null;

    try {
      File file = _getFile(fileId);
      unpinned = file.exists();
      if( unpinned )
        pinId = fileId;
      else
        reason = "file does not exist";
    }
    catch (Exception ex) {
      reason = "got exception " + ex;
      elog( ex );
    }

   if( unpinned )
     callbacks.Unpinned( pinId );
   else
     callbacks.UnpinningFailed( reason );
  }

  /** */
  public void advisoryDelete(SRMUser user, String filePath,
                             AdvisoryDeleteCallbacks callbacks) {
    /**@todo advisoryDelete() - user, async */
    if( ! ( callbacks instanceof AdvisoryDeleteCallbacks ) )
      throw new java.lang.IllegalArgumentException(
          "Method advisoryDelete() has wrong callback argument type.");

    boolean deleted = false;
    String  reason  = null;

    try {
      /**@todo advisoryDelete() - _getFile(filePath) assumes fileID == filePath */
      // fileId is Canonical path
      File file = _getFile(filePath);
      deleted   = file.delete();
      if( ! deleted )
        reason = "delete file operation failed";
    }
    catch (Exception ex) {
      reason = "got exception " + ex;
      elog( ex );
    }

    if( deleted )
      callbacks.AdvisoryDeleteSuccesseded();
    else
      callbacks.AdvisoryDeleteFailed( reason );

  }

  /**
   * _installPath checks that
   * - path is directory path and it exists
   * If it does not exist it checks parent directories recurcevely
   *   and creates missing path elements starting from existing elemant in the path
   *   down to this "path"
   * */

  private boolean _installPath(SRMUser user, String path) {
    boolean exist;
    log("_installPath("+user+","+path+")");
    File file   = new File(path);

    if ( file.exists() ) {
    log("_installPath: file exists, returning "+(file.isDirectory() && file.canWrite() ));
      return ( file.isDirectory() && file.canWrite() );
    }

    // If this dir does not exist,
    //   check / create leading path elements
    String pPath = file.getParent();
    if( pPath == null )
      return false;

    if( ! _installPath(user, pPath ) )
      return false;

    //   and create directory itself
    return file.mkdir();
  }

  /** */
  public void prepareToPut(SRMUser user, String filePath,
                           PrepareToPutCallbacks callbacks,
                           boolean overwrite ) {
   // the type of callback is already specified in the function declaration                            
   // if( ! ( callbacks instanceof PrepareToPutCallbacks ) )
   //   throw new java.lang.IllegalArgumentException(
   //       "Method prepareToPut() has wrong callback argument type.");

    String       parentPath;
    File         file   = null;
    File         parent = null;

    String       fileId = null;
    FileMetaData fmd    = null;

    String parentFileId = null;
    FileMetaData parentFmd =null;

    String path = null;

    try {
      path   = filePath;           //for error report
      file   = new File(filePath);

      if( file.exists() ) {
          if(overwrite) {
                fmd    = _getFileMetaData( user, filePath );
                fileId = filePath;//_getFileId( fmd );
                parentFmd = null;
                parentFileId = null;
          } else {
             String erStr = "file exists, can't overwrite";
            callbacks.GetStorageInfoFailed( erStr );
             return;              
          }
      } else {
        parentPath = file.getParent();  //for error report
        path   = parentPath;
        if(  _installPath(user,parentPath) ) {
          fileId = null;
          fmd = null; // ignored in callback
          parentFmd = _getFileMetaData(user, parentPath);
          parentFileId = parentPath;//_getFileId(parentFmd);
        }else{
          String erStr = "prepareToPut() can not get or create parent for the filePath="
              +filePath +".";
          callbacks.GetStorageInfoFailed( erStr );
          return;
        }
      }
    }
    catch (Exception ex) {
      elog( ex );
      String erStr = "prepareToPut() got exception for the filePath=" +path +".";
      callbacks.GetStorageInfoFailed( erStr );
      return;
    }

    dlog( "prepareToPut(): StorageInfoArrived, fileId="+fileId + "fmd=" + fmd );
    callbacks.StorageInfoArrived(fileId, fmd,
                                 parentFileId, parentFmd);
  }

  /**
   * Not implemented.<br>
   * This is a feature of SRM interface v2.0
   * */
  public void prepareToPutInReservedSpace(SRMUser user, String path, long size, long spaceReservationToken, PrepareToPutInSpaceCallbacks callbacks) {
    /**@todo SRM v2.0 Implement prepareToPutInReservedSpace() */
    if( ! ( callbacks instanceof PrepareToPutInSpaceCallbacks )  )
      throw new java.lang.IllegalArgumentException(
          "Method prepareToPutInReservedSpace() has wrong callback argument type.");

    Exception eex = new UnsupportedOperationException(
        "Method prepareToPutInReservedSpace() not yet implemented, this is the feature of SRM interface v2.0.");
    elog(eex);
    callbacks.Exception(eex);
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

  //----------------------------------------------------------------------------
  // srm.Logger Interface implementation

  /** */
  public void log(String s) {
    out.println(new java.util.Date().toString() + " " + s);
  }

  /** */
  public void elog(String s) {
    err.println(new java.util.Date().toString() + " " + s);
  }

  /** */
  public void elog(Throwable t) {
    t.printStackTrace(err);
  }

  /** */
  public void dlog(String s) {
    if( debug )
      log( s );
  }

  /** */
  public void dlog(Throwable t) {
    if( debug )
      elog(t);
  }
  
  private void changeOwnership(String filePath, int uid, int gid) throws IOException {
      StringWriter out = new StringWriter();
      StringWriter err = new StringWriter();
       
      File file = new File(filePath);
      if(!file.exists()) throw new IOException("file does not exist");
      String command = chown_cmd+" "+uid+"."+gid+" "+file.getCanonicalPath();
      log("executing command "+command);
      int return_code = ShellCommandExecuter.execute(command,out, err);
      log("command standard output:"+out.getBuffer().toString());
      if(return_code != 0 ) {
          log("command error    output:"+err.getBuffer().toString());
          throw new IOException ("command failed with return_code="+return_code);
      }

  }
  
  private static long unique_id = 0;
  private static synchronized final String getUniqueId() {
      return Long.toHexString(unique_id++);
      
  }
  private Map copyThreads = new HashMap();
  
  public String getFromRemoteTURL(SRMUser user, String remoteTURL, String actualFilePath, SRMUser remoteUser, Long remoteCredentialId,  CopyCallbacks callbacks) throws SRMException{
   return this.getFromRemoteTURL( user,  remoteTURL,  actualFilePath,  remoteUser,  remoteCredentialId,  null,0, callbacks);
  
  }
 
  /**
     * @param user User ID
     * @param remoteTURL
     * @param actualFilePath
     * @param remoteUser
     * @param remoteCredetial
     * @param callbacks
     * @throws SRMException
     * @return transfer id
     */
    public String getFromRemoteTURL(
        final SRMUser user, 
        final String remoteTURL, 
        final String actualFilePath, 
        final SRMUser remoteUser, 
        final Long remoteCredentialId, 
        String spaceReservationId, 
        long size, 
        final CopyCallbacks callbacks) throws SRMException{
        Thread t = new Thread(){
            
            public void run() {
                try
                {
                    log("calling getFromRemoteTURL from a copy thread");
                    getFromRemoteTURL(user, remoteTURL,actualFilePath, remoteUser, remoteCredentialId);
                    log("calling callbacks.copyComplete for path="+actualFilePath);
                    callbacks.copyComplete(actualFilePath, getFileMetaData(user,actualFilePath));
                }
                catch (Exception e){
                    callbacks.copyFailed(e);
                }
            }
        };
        String id = getUniqueId();
        log("getFromRemoteTURL assigned id ="+id+"for transfer from "+remoteTURL+" to "+actualFilePath);
        
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
    public String putToRemoteTURL(final SRMUser user, final String actualFilePath,final String remoteTURL, final SRMUser remoteUser, final Long remoteCredentialId, final CopyCallbacks callbacks)
    throws SRMException {
        
       Thread t = new Thread(){
            
            public void run() {
                try
                {
                    log("calling putToRemoteTURL from a copy thread");
                    putToRemoteTURL(user, actualFilePath,remoteTURL, remoteUser, remoteCredentialId);
                    log("calling callbacks.copyComplete for path="+actualFilePath);
                    callbacks.copyComplete(actualFilePath, getFileMetaData(user,actualFilePath));
                }
                catch (Exception e){
                    callbacks.copyFailed(e);
                }
            }
        };
        String id = getUniqueId();
        log("putToRemoteTURL assigned id ="+id+"for transfer from "+actualFilePath+" to "+remoteTURL);
        
        copyThreads.put(id, t);
        t.start();
        return id;
    }
   

  public void killRemoteTransfer(String transferId) {
      Thread t = (Thread) copyThreads.get(transferId);
      if(t == null) {
          log("killRemoteTransfer: cannot find thread for transfer with id="+ transferId);
      }
      else
      {
          log("killRemoteTransfer: found thread for transfer with id="+ transferId+", killing");
          t.interrupt();
      }
  }
  
  public void reserveSpace(SRMUser user, long spaceSize, long reservationLifetime, String filename, String host, ReserveSpaceCallbacks callbacks) {
      callbacks.SpaceReserved("dummy", spaceSize);
  }
  
  

      public void removeFile(final SRMUser user, 
			     final String path, 
			     RemoveFileCallbacks callbacks) {
      }

      public void removeDirectory(final SRMUser user, 
				  final Vector tree)  throws SRMException {
      }

      public void createDirectory(final SRMUser user, 
				  final String directory) throws SRMException {
      }

	public String[] listNonLinkedDirectory(SRMUser user, String directoryName) throws SRMException {
         FileMetaData fmd = getFileMetaData(user, directoryName);
         int uid = Integer.parseInt(fmd.owner);
         int gid = Integer.parseInt(fmd.group);
         int permissions = fmd.permMode;
         
         if(permissions == 0 ) {
            throw new SRMException ("permission denied");
         }
         
         if(!Permissions.worldCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }
         
         if(uid == -1 || gid == -1) {
            throw new SRMException ("permission denied");
         }
         
         if(user == null ) {
            throw new SRMException ("permission denied");
         }
         
         if(!fmd.isGroupMember(user)  || ! Permissions.groupCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }
         
         if(!fmd.isOwner(user)  || ! Permissions.userCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }
         File f = new File(directoryName);
         if(!f.isDirectory()) {
             throw new SRMException("not a directory");
         }
         return f.list();

	}
  
      public FileMetaData getFileMetaData(SRMUser user, String filePath, FileMetaData parentFMD) throws SRMException {
        /**@todo getFileMetaData() - process exception */
        return _getFileMetaData(user, filePath);
      }
      
      public java.io.File[] listDirectoryFiles(SRMUser user, String directoryName, FileMetaData fileMetaData) throws SRMException {
         FileMetaData fmd = getFileMetaData(user, directoryName);
         int uid = Integer.parseInt(fmd.owner);
         int gid = Integer.parseInt(fmd.group);
         int permissions = fmd.permMode;
         
         if(permissions == 0 ) {
            throw new SRMException ("permission denied");
         }
         
         if(!Permissions.worldCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }
         
         if(uid == -1 || gid == -1) {
            throw new SRMException ("permission denied");
         }
         
         if(user == null ) {
            throw new SRMException ("permission denied");
         }
         
         if(!fmd.isGroupMember(user) || !Permissions.groupCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }
         
         if(!fmd.isOwner(user) || !Permissions.userCanRead(permissions)) {
            throw new SRMException ("permission denied");
         }
         File f = new File(directoryName);
         if(!f.isDirectory()) {
             throw new SRMException("not a directory");
         }
         return f.listFiles();

      }

      public String[] listDirectory(SRMUser user, String directoryName, FileMetaData fileMetaData) throws SRMException {
         FileMetaData fmd = getFileMetaData(user, directoryName);
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
         return f.list();

      }

	public void moveEntry(SRMUser user, String from,
			       String to) throws SRMException { 
	}

    public void srmReserveSpace(SRMUser user, long sizeInBytes, long spaceReservationLifetime, String retentionPolicy, String accessLatency, String description,org.dcache.srm.SrmReserveSpaceCallbacks callbacks) {
    }

    public void srmUnmarkSpaceAsBeingUsed(SRMUser user, String spaceToken, String fileName, SrmCancelUseOfSpaceCallbacks callbacks) {
    }

    public void srmReleaseSpace(SRMUser user, String spaceToken, Long sizeInBytes, SrmReleaseSpaceCallbacks callbacks) {
    }

    public void srmMarkSpaceAsBeingUsed(SRMUser user, String spaceToken, String fileName, long sizeInBytes, long useLifetime, 
        boolean overwrite,
        SrmUseSpaceCallbacks callbacks) {
    }
 

    /**
     * 
     * @param spaceTokens 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public TMetaDataSpace[] srmGetSpaceMetaData(SRMUser user, String[] spaceTokens)
        throws SRMException {
        return null;
    }
    /**
     * 
     * @param description 
     * @throws org.dcache.srm.SRMException 
     * @return 
     */
    public String[] srmGetSpaceTokens(SRMUser user,String description)
        throws SRMException {
        return null;
    }
    
    public String[] srmGetRequestTokens(SRMUser user,String description)
        throws SRMException{
        return null;
    }

    /**
     * 
     * 
     * @param newLifetime SURL lifetime in seconds
     *   -1 stands for infinite lifetime
     * @return long lifetime left in seconds
     *   -1 stands for infinite lifetime
     */
    public int srmExtendSurlLifetime(SRMUser user, String fileName, int newLifetime) throws SRMException {
        FileMetaData fmd = getFileMetaData(user,fileName);
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

    public long extendPinLifetime(SRMUser user, String fileId, String pinId, long newPinLifetime) throws SRMException {
        return newPinLifetime;
    }

    public long srmExtendReservationLifetime(SRMUser user, String spaceToken, long newReservationLifetime) throws SRMException {
        return newReservationLifetime;
    }
    
    public String getStorageBackendVersion() { return "$Revision: 1.35 $"; } 
    
    public void unPinFileBySrmRequestId(SRMUser user,String fileId,
        UnpinCallbacks callbacks,
        long srmRequestId)   {
        callbacks.Unpinned(fileId);
    }
    
   /** This method allows to unpin file in the Storage Element,
     * i.e. cancel the requests to have the file in "fast access state"
     * This method will remove all pins on this file user has permission 
     * to remove
     * @param user User ID
     * @param fileId Storage Element internal file ID
     * @param callbacks This interface is used for asyncronous notification of SRM of the
     * various actions performed to "unpin" file in the storage
     * @param srmRequestId id given to the storage  during pinFile operation 
     */
    public void unPinFile(SRMUser user,String fileId,
            UnpinCallbacks callbacks) {
        callbacks.Unpinned(fileId);
        
    }
    public boolean exists(SRMUser user, String path)  throws SRMException { 
            return true;
    }
}
