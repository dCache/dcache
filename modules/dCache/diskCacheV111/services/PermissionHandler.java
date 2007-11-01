// $Id: PermissionHandler.java,v 1.8 2005-11-17 20:48:41 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.7  2005/11/10 23:00:19  timur
// better faster srm ls in non verbose mode
//
// Revision 1.6  2005/11/10 00:01:42  timur
// new methods that will allow the reuse of file metadata for reduction of pnfs operations
//
// Revision 1.5  2004/06/03 12:33:08  tigran
// added mkdir rmdir
// PnfsHandler.deleteEntry throws exception in case of non empty directory
//
// Revision 1.4  2004/04/16 12:03:18  cvs
// added candRead with storageInfo
//
// Revision 1.3  2003/12/08 17:34:05  cvs
// added canDelete() to permissionHandler, use it in ftp door
//
// Revision 1.2  2003/10/03 14:25:40  cvs
// added worldCanRead and worldCanWrite fncs
//

package diskCacheV111.services;

import java.util.* ;
import dmg.cells.nucleus.* ;
import diskCacheV111.vehicles.* ;
import diskCacheV111.util.*;

public class PermissionHandler {

   private CellAdapter _cell ;
   private PnfsHandler _handler;
   private static final int __pnfsTimeout = 5 * 60 * 1000 ;
                                
   
   public final void say(String s)
   {
       if(_cell != null)  _cell.say("PermissionHandler: "+s);
   }
   
   public void esay(String s)
   {
       if(_cell != null)  _cell.esay("PermissionHandler: "+s);
   }
   
   public PermissionHandler(CellAdapter parent, CellPath pnfsManagerPath){
                             
       _cell     = parent ;
       _handler = new PnfsHandler(parent,pnfsManagerPath);
                                
   }

   public PermissionHandler(CellAdapter parent, PnfsHandler handler){
                             
       _cell     = parent ;
       _handler = handler;
                                
   }   
   
   //make it final to allow compile time inlining
   public final boolean writeAllowed(int userUid,int userGid,FileMetaData meta ) {
      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      return ( ( meta.getUid() == userUid ) && user.canWrite()  ) ||
        ( ( meta.getGid() == userGid ) && group.canWrite() ) ||
            world.canWrite() ;
  }
  
  public final boolean executeAllowed(int userUid,int userGid,FileMetaData meta ) {
      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      return ( ( meta.getUid() == userUid ) && user.canExecute()  ) ||
        ( ( meta.getGid() == userGid ) && group.canExecute() ) ||
            world.canExecute() ;
  }
  
  public final boolean readAllowed(int userUid,int userGid,FileMetaData meta ) {
      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      return ( ( meta.getUid() == userUid ) && user.canRead()  ) ||
        ( ( meta.getGid() == userGid ) && group.canRead() ) ||
            world.canRead()  ;
  }
  
  public final boolean readAndExecuteAllowed(int userUid,int userGid,FileMetaData meta ) {
      return readAllowed(userUid, userGid, meta) &&
      executeAllowed(userUid, userGid, meta);
  }
  
  public final boolean writeAndExecuteAllowed(int userUid,int userGid,FileMetaData meta ) {
      return writeAllowed(userUid, userGid, meta) &&
      executeAllowed(userUid, userGid, meta);
  }
  
  public final boolean readAndWriteAndExecuteAllowed(int userUid,int userGid,FileMetaData meta ) {
      return readAllowed(userUid, userGid, meta) &&
      writeAllowed(userUid, userGid, meta) &&
      executeAllowed(userUid, userGid, meta);
  }
  
   /**
    * checks is the user with uid = userUid and gid = userGid can write 
    * into a file with pnfs path = pnfsPath
    */
   
  public boolean canWrite(int userUid,int userGid, String pnfsPath) throws CacheException
  {
     say("canWrite("+userUid+","+userGid+","+pnfsPath+")");
     PnfsGetFileMetaDataMessage info;
     try
     {
      info = _handler.getFileMetaDataByPath(pnfsPath);
          // file exists, can not write
          esay(pnfsPath + " exists, can not write");
          return false;
     }
     catch(CacheException ce)
     {
         say("received CacheException => file does not exists, continue");
     }
     
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     return canWriteIntoDir(userUid, userGid,parent);
  }
  
  public boolean canWriteIntoDir(int userUid,int userGid, String dirPath)
  throws CacheException
  {
     PnfsGetFileMetaDataMessage info = _handler.getFileMetaDataByPath(dirPath);
     if(info.getReturnCode() != 0)
      {
          // file exists, can not write
          esay("directory " + dirPath + " does not exists, can not write");
          return false;
      }
      
      FileMetaData meta = info.getMetaData();
      if(!meta.isDirectory())
      {
          esay(dirPath +" exists and is not a directory, can not write into it ");
          return false;
      }
      return writeAndExecuteAllowed(userUid, userGid, meta);
  }
  
  
  public boolean canCreateDir(int userUid,int userGid, String pnfsPath) throws CacheException
  {
     say("canCreateDir("+userUid+","+userGid+","+pnfsPath+")");
     PnfsGetFileMetaDataMessage info;
     
     try
     {
      info = _handler.getFileMetaDataByPath(pnfsPath);
          // file exists, can not write
          esay(pnfsPath + " exists, can not create");
          return false;
     }
     catch(CacheException ce)
     {
         say("received CacheException => file does not exists, continue");
     }
     
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     return canWriteIntoDir(userUid, userGid,parent);
  }  
  
  
  public boolean canDeleteDir(int userUid,int userGid, String pnfsPath) throws CacheException
  {
     say("canDeleteDir("+userUid+","+userGid+","+pnfsPath+")");
     PnfsGetFileMetaDataMessage info = null;
     try
     {
        info = _handler.getFileMetaDataByPath(pnfsPath);          
     }
     catch(CacheException ce)
     {
         esay(pnfsPath + ": does not exist");
         throw new CacheException("path does not exist" );
     }
     
      
      FileMetaData meta = info.getMetaData();
      if(!meta.isDirectory())
      {
          esay(pnfsPath +" is not a directory");
          throw new CacheException( "path is not a directory" );
      }
      
      boolean writeAllowed = writeAllowed(userUid, userGid, meta);
      return writeAllowed;
  }  
  
  public boolean canDelete(int userUid,int userGid, String pnfsPath) throws CacheException
  {
     say("canDelete("+userUid+","+userGid+","+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     boolean parentCandWrite = canWriteIntoDir(userUid, userGid,parent);
     say("canDelete() can write into parent:"+parentCandWrite);

      if(!parentCandWrite )
      {
          esay(" parent write is not allowed ");
          return false;
      }
     
      PnfsGetFileMetaDataMessage info = _handler.getFileMetaDataByPath(pnfsPath);
      if(info.getReturnCode() != 0)
      {
          // file exists, can not write
          esay("canDelete() : " +pnfsPath + " does not exists => can not delete");
          return false;
      }
      FileMetaData meta = info.getMetaData();
      say("canDelete() file meta = "+meta); 
      boolean deleteAllowed = writeAllowed(userUid, userGid, meta);
      if(deleteAllowed)
      {
            esay("WARNING: canDelete() delete of file "+pnfsPath+
            " by user uid="+userUid+" gid="+userGid+" is allowed!");
      }
      else
      {
          say("canDelete() delete of file "+pnfsPath+
            " by user uid="+userUid+" gid="+userGid+" is not allowed");
      }
      return deleteAllowed;
  }
  
  public boolean canRead(int userUid,int userGid, String pnfsPath) throws CacheException
  {

      PnfsGetFileMetaDataMessage info = _handler.getFileMetaDataByPath(pnfsPath);
      if(info.getReturnCode() != 0)
      {
          // file exists, can not write
          esay(pnfsPath + " does not exists => can not read");
          return false;
      }
            
       FileMetaData meta = info.getMetaData();
      return this.canRead(userUid, userGid, pnfsPath, meta );
  }

  public boolean canRead(int userUid,int userGid, String pnfsPath, FileMetaData meta) throws CacheException
  {
    // to skeep communication with pnfs, if file storage info already known
    // we will check file permission first and then parent permission     
           
    say("canRead("+userUid+","+userGid+","+pnfsPath+")");
      
      if ( ! this.fileCanRead(userUid, userGid, meta ) ) {
          return false;
      }
     
     
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");          
     String parent = parent_path.toString();
     
     return  dirCanRead( userUid, userGid, parent);
  }  

  public boolean canRead(int userUid,int userGid, String pnfsPath,FileMetaData parentMeta, FileMetaData meta) throws CacheException
  {
    // to skeep communication with pnfs, if file storage info already known
    // we will check file permission first and then parent permission     
           
    say("canRead("+userUid+","+userGid+","+pnfsPath+")");
      
      if ( ! this.fileCanRead(userUid, userGid, meta ) ) {
          return false;
      }
     
     
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");          
     String parent = parent_path.toString();
     
     return  dirCanRead( userUid, userGid, parent, parentMeta);
  }  
  
  private boolean dirCanRead( int userUid, int userGid, String path ) throws CacheException {

     PnfsGetFileMetaDataMessage info = _handler.getFileMetaDataByPath(path);
     FileMetaData meta = info.getMetaData();
     return dirCanRead(userUid,userGid,path,meta);
  }
  
  public boolean dirCanRead( int userUid, int userGid, String path ,FileMetaData meta) throws CacheException {

     say("dirCanRead() meta = "+meta); 
     if(meta == null) {
         esay(path +" does not exists, can not read ");
         return false;
     }
      if(!meta.isDirectory())
      {
          esay(path +" exists and is not a directory, can not read ");
          return false;
      }
      boolean readAndExecuteAllowed = readAndExecuteAllowed(userUid, userGid, meta);
      say("dirCanRead() read and execute allowed :"+readAndExecuteAllowed);

      if(!readAndExecuteAllowed)
      {
          esay(" read is not allowed ");
          return false;
      }

     return true;
     
  }
  
  private boolean fileCanRead( int userUid, int userGid, FileMetaData meta) {
      
      say("fileCanRead() file meta = "+meta);
      
      boolean readAllowed = readAllowed(userUid, userGid, meta);
      
      say("fileCanRead() file read allowed : "+readAllowed);      
      return readAllowed;
      
  }
  
  
  public boolean worldCanRead(String pnfsPath) throws CacheException
  {
     say("worldCanRead("+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     PnfsGetFileMetaDataMessage info = _handler.getFileMetaDataByPath(parent);
     FileMetaData meta = info.getMetaData();
     say("worldCanRead() parent meta = "+meta); 
      if(!meta.isDirectory())
      {
          esay(parent +" exists and is not a directory, can not read "+pnfsPath);
          return false;
      }
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      boolean parentReadAllowed =      world.canRead() ;
      boolean parentExecuteAllowed =   world.canExecute() ;
     say("worldCanRead() parent read allowed :"+parentReadAllowed+
     " parent exec allowed :"+parentExecuteAllowed);

      if(!parentReadAllowed || ! parentExecuteAllowed)
      {
          esay(" parent read is not allowed ");
          return false;
      }
     
      info = _handler.getFileMetaDataByPath(pnfsPath);
      if(info.getReturnCode() != 0)
      {
          // file exists, can not write
          esay(pnfsPath + " does not exists => can not read");
          return false;
      }
      meta = info.getMetaData();
      say("worldCanRead() file meta = "+meta); 
      world = meta.getWorldPermissions() ;
      boolean readAllowed = 
            world.canRead() ;
      
     say("worldCanRead() file read allowed :"+readAllowed);
      return readAllowed;
  }
  
  public boolean worldCanRead(String pnfsPath,FileMetaData parentmeta,FileMetaData meta) throws CacheException
  {
     say("worldCanRead("+pnfsPath+","+parentmeta+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     say("worldCanRead() parent meta = "+parentmeta); 
      if(parentmeta == null){
          esay(parent +"does not exist  can not read "+pnfsPath);
          return false;
          
      }
      if(!parentmeta.isDirectory())
      {
          esay(parent +" exists and is not a directory, can not read "+pnfsPath);
          return false;
      }
      FileMetaData.Permissions world = parentmeta.getWorldPermissions() ;
      boolean parentReadAllowed =      world.canRead() ;
      boolean parentExecuteAllowed =   world.canExecute() ;
      say("worldCanRead() parent read allowed :"+parentReadAllowed+
      " parent exec allowed :"+parentExecuteAllowed);

      if(!parentReadAllowed || ! parentExecuteAllowed)
      {
          esay(" parent read is not allowed ");
          return false;
      }
     
      if(meta == null){
          // file exists, can not write
          esay(pnfsPath + " does not exists => can not read");
          return false;
      }
      say("worldCanRead() file meta = "+meta); 
      world = meta.getWorldPermissions() ;
      boolean readAllowed = 
            world.canRead() ;
      
     say("worldCanRead() file read allowed :"+readAllowed);
      return readAllowed;
  }

  public boolean worldCanWrite(String pnfsPath) throws CacheException
  {
      //simple and elegant
      return false;
  }
}
