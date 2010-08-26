// $Id: FsPermissionHandler.java,v 1.4 2006-11-07 10:41:59 tigran Exp $

package diskCacheV111.services;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import dmg.cells.nucleus.CellAdapter;

public class FsPermissionHandler {

   private CellAdapter _cell ;
   FileMetaDataSource _metaDataSource;

   public void say(String s)
   {
       if(_cell != null)  _cell.say("PermissionHandler: "+s);
   }
   
   public void esay(String s)
   {
       if(_cell != null)  _cell.esay("PermissionHandler: "+s);
   }
   
   public FsPermissionHandler(CellAdapter cell, FileMetaDataSource metaDataSource){                             
       _cell     = cell ;
       _metaDataSource = metaDataSource;
   }
   
   /**
    * checks is the user with uid = userUid and gid = userGid can write 
    * into a file with pnfs path = pnfsPath
    */
   
  public boolean canWrite(int userUid,int userGid, String pnfsPath) throws CacheException {
     say("canWrite("+userUid+","+userGid+","+pnfsPath+")");
     
     try {         
         return fileCanWrite(userUid, userGid, _metaDataSource.getMetaData(pnfsPath) );         
     }catch( CacheException ce){
         // file do not exist, check directory
     }
     
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");
   
     String parent = parent_path.toString();
      FileMetaData meta = _metaDataSource.getMetaData(parent);
      if( !meta.isDirectory() ) {
          esay(parent +
               " exists and is not a directory, can not create " + 
               pnfsPath);
          return false;
      }
      
      boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta); 
      boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);
      
      return parentWriteAllowed && parentExecuteAllowed;
  }  
  
  public boolean canCreateDir(int userUid,int userGid, String pnfsPath) throws CacheException  {
     say("canCreateDir("+userUid+","+userGid+","+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
      FileMetaData meta = _metaDataSource.getMetaData(parent);
      if( !meta.isDirectory() ) {
          esay(parent +" exists and is not a directory, can not create "+pnfsPath);
          return false;
      }
            
      boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);      
      boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);
                  
      return parentWriteAllowed && parentExecuteAllowed;
  }  
  
  public boolean canDeleteDir(int userUid,int userGid, String pnfsPath) throws CacheException
  {
     say("canDeleteDir("+userUid+","+userGid+","+pnfsPath+")");
      
      FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
      if( !meta.isDirectory() ) {          
          esay(pnfsPath +" is not a directory");
          throw new CacheException( "path is not a directory" );
      }
            
      return fileCanWrite(userUid, userGid, meta);
  }  
  
  public boolean canDelete(int userUid,int userGid, String pnfsPath) throws CacheException  {
     say("canDelete("+userUid+","+userGid+","+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     FileMetaData meta = _metaDataSource.getMetaData(parent);
     say("canWrite() parent meta = "+meta); 
      if( !meta.isDirectory() ) {      
          esay(parent +" exists and is not a directory, can not read "+pnfsPath);
          return false;
      }
                  
      boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);      
      boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);
      boolean parentReadAllowed = fileCanRead(userUid, userGid, meta);                  
      
     say("canDelete() parent read allowed :"+parentReadAllowed+
     " parent write allowed :"+parentReadAllowed+
     " parent exec allowed :"+parentExecuteAllowed);

      if(!parentReadAllowed || ! parentExecuteAllowed || ! parentWriteAllowed ) {
          esay(" parent write is not allowed ");
          return false;
      }
     
      meta = _metaDataSource.getMetaData(pnfsPath);
     say("canDelete() file meta = "+meta); 

     boolean deleteAllowed = fileCanWrite(userUid, userGid, meta) ;
      
      if( deleteAllowed )  {
            esay("WARNING: canDelete() delete of file "+pnfsPath+
            " by user uid="+userUid+" gid="+userGid+" is allowed!");
      } else {
          say("canDelete() delete of file "+pnfsPath+
            " by user uid="+userUid+" gid="+userGid+" is not allowed");
      }
      return deleteAllowed;
  }
  
  public boolean canRead(int userUid,int userGid, String pnfsPath) throws CacheException  {
           
    say("canRead("+userUid+","+userGid+","+pnfsPath+")");
       
      if ( ! fileCanRead(userUid, userGid, _metaDataSource.getMetaData(pnfsPath) ) ) {
          return false;
      }     
     
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");          
     String parent = parent_path.toString();
     
     return  dirCanRead( userUid, userGid, parent);
  }  

  
  private boolean dirCanRead( int userUid, int userGid, String path ) throws CacheException {

     FileMetaData meta = _metaDataSource.getMetaData(path);
     say("dirCanRead() meta = "+meta); 
      if( !meta.isDirectory() ) {
          esay(path +" exists and is not a directory, can not read ");
          return false;
      }
            
      boolean readAllowed = fileCanRead(userUid, userGid, meta);;
      boolean executeAllowed = fileCanExecute(userUid, userGid, meta);;
      
     say("dirCanRead() read allowed :"+readAllowed+
     "  exec allowed :"+executeAllowed);

      if( !(readAllowed && executeAllowed) ) {
          esay(" read is not allowed ");
          return false;
      }

     return readAllowed && executeAllowed;
  }
    
  public boolean worldCanRead(String pnfsPath) throws CacheException
  {
     say("worldCanRead("+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     FileMetaData meta = _metaDataSource.getMetaData(parent);
     say("worldCanRead() parent meta = "+meta); 
      if( !meta.isDirectory() ) {
          esay(parent +" exists and is not a directory, can not read "+pnfsPath);
          return false;
      }
      
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      boolean parentReadAllowed =      world.canRead() ;
      boolean parentExecuteAllowed =   world.canExecute() ;
     say("worldCanRead() parent read allowed :"+parentReadAllowed+
     " parent exec allowed :"+parentExecuteAllowed);

      if(!parentReadAllowed || ! parentExecuteAllowed) {
          esay(" parent read is not allowed ");
          return false;
      }
     
      meta = _metaDataSource.getMetaData(pnfsPath);
      say("worldCanRead() file meta = "+meta); 
      world = meta.getWorldPermissions() ;
      boolean readAllowed = world.canRead() ;
      
     say("worldCanRead() file read allowed :"+readAllowed);
      return readAllowed;
  }
  
  public boolean worldCanWrite(String pnfsPath) throws CacheException  {
      //simple and elegant
      return false;
  }  
  
  //////////////////////////////////////////////////////////////////////////////////
  ///
  ///  Low level checks
  ///
  /////////////////////////////////////////////////////////////////////////////////

  
  private static boolean fileCanWrite( int userUid, int userGid, FileMetaData meta) {

      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      
      boolean writeAllowed = false;
      
      if( meta.getUid() == userUid ) {
          writeAllowed = user.canWrite();
      } else if (meta.getGid() == userGid ) {
          writeAllowed = group.canWrite();
      }else {
          // world = all except user and group
          writeAllowed = world.canWrite() ;
      }      
            
      return writeAllowed;      
  }
  
  
  private static boolean fileCanExecute( int userUid, int userGid, FileMetaData meta) {
      
      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      
      boolean writeAllowed = false;
      
      if( meta.getUid() == userUid ) {
          writeAllowed = user.canExecute();
      } else if (meta.getGid() == userGid ) {
          writeAllowed = group.canExecute();
      }else {
          // world = all except user and group
          writeAllowed = world.canExecute() ;
      }      
            
      return writeAllowed;      
  }  
  
  private static boolean fileCanRead( int userUid, int userGid, FileMetaData meta) {
      
      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      
      boolean readAllowed = false;
      
      if( meta.getUid() == userUid ) {
          readAllowed = user.canRead();
      } else if (meta.getGid() == userGid ) {
          readAllowed = group.canRead();
      }else {
          // world = all except user and group
          readAllowed = world.canRead() ;
      }      
            
      return readAllowed;      
  }
    
  
}
