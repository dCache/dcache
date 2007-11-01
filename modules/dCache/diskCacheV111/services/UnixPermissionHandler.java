// $Id: UnixPermissionHandler.java,v 1.2 2007-10-23 15:24:02 tigran Exp $

package diskCacheV111.services;

import org.apache.log4j.Logger;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import dmg.cells.nucleus.CellAdapter;

public class UnixPermissionHandler implements PermissionHandlerInterface {

   private CellAdapter _cell ;
   private final FileMetaDataSource _metaDataSource;

   private final static Logger _logPermisions = Logger
   .getLogger("logger.org.dcache.authorization."
           + UnixPermissionHandler.class.getName());

   /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#say(java.lang.String)
 */
   public UnixPermissionHandler(CellAdapter cell, FileMetaDataSource metaDataSource){
       _cell     = cell ;
       _metaDataSource = metaDataSource;
   }

   /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#canWrite(int, int, java.lang.String)
 */

  public boolean canWrite(int userUid,int[] userGid, String pnfsPath) throws CacheException {
    _logPermisions.debug("canWrite("+userUid+","+userGid+","+pnfsPath+")");

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
         _logPermisions.error(parent +
               " exists and is not a directory, can not create " +
               pnfsPath);
          return false;
      }

      boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);
      boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);

      return parentWriteAllowed && parentExecuteAllowed;
  }

  /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#canCreateDir(int, int, java.lang.String)
 */
public boolean canCreateDir(int userUid,int[] userGid, String pnfsPath) throws CacheException  {
    _logPermisions.debug("canCreateDir("+userUid+","+userGid+","+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
      FileMetaData meta = _metaDataSource.getMetaData(parent);
      if( !meta.isDirectory() ) {
         _logPermisions.error(parent +" exists and is not a directory, can not create "+pnfsPath);
          return false;
      }

      boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);
      boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);

      return parentWriteAllowed && parentExecuteAllowed;
  }

  /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#canDeleteDir(int, int, java.lang.String)
 */
public boolean canDeleteDir(int userUid,int[] userGid, String pnfsPath) throws CacheException
  {
    _logPermisions.debug("canDeleteDir("+userUid+","+userGid+","+pnfsPath+")");

      FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
      if( !meta.isDirectory() ) {
         _logPermisions.error(pnfsPath +" is not a directory");
          throw new CacheException( "path is not a directory" );
      }

      return fileCanWrite(userUid, userGid, meta);
  }

  /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#canDelete(int, int, java.lang.String)
 */
public boolean canDelete(int userUid,int[] userGid, String pnfsPath) throws CacheException  {
    _logPermisions.debug("canDelete("+userUid+","+userGid+","+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     FileMetaData meta = _metaDataSource.getMetaData(parent);
    _logPermisions.debug("canWrite() parent meta = "+meta);
      if( !meta.isDirectory() ) {
         _logPermisions.error(parent +" exists and is not a directory, can not read "+pnfsPath);
          return false;
      }

      boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);
      boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);
      boolean parentReadAllowed = fileCanRead(userUid, userGid, meta);

    _logPermisions.debug("canDelete() parent read allowed :"+parentReadAllowed+
     " parent write allowed :"+parentReadAllowed+
     " parent exec allowed :"+parentExecuteAllowed);

      if(!parentReadAllowed || ! parentExecuteAllowed || ! parentWriteAllowed ) {
         _logPermisions.error(" parent write is not allowed ");
          return false;
      }

      meta = _metaDataSource.getMetaData(pnfsPath);
    _logPermisions.debug("canDelete() file meta = "+meta);

     boolean deleteAllowed = fileCanWrite(userUid, userGid, meta) ;

      if( deleteAllowed )  {
           _logPermisions.error("WARNING: canDelete() delete of file "+pnfsPath+
            " by user uid="+userUid+" gid="+userGid+" is allowed!");
      } else {
         _logPermisions.debug("canDelete() delete of file "+pnfsPath+
            " by user uid="+userUid+" gid="+userGid+" is not allowed");
      }
      return deleteAllowed;
  }

  /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#canRead(int, int, java.lang.String)
 */
public boolean canRead(int userUid,int[] userGid, String pnfsPath) throws CacheException  {

   _logPermisions.debug("canRead("+userUid+","+userGid+","+pnfsPath+")");

      if ( ! fileCanRead(userUid, userGid, _metaDataSource.getMetaData(pnfsPath) ) ) {
          return false;
      }

     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");
     String parent = parent_path.toString();

     return  dirCanRead( userUid, userGid, parent);
  }


  private boolean dirCanRead( int userUid, int[] userGid, String path ) throws CacheException {

     FileMetaData meta = _metaDataSource.getMetaData(path);
    _logPermisions.debug("dirCanRead() meta = "+meta);
      if( !meta.isDirectory() ) {
         _logPermisions.error(path +" exists and is not a directory, can not read ");
          return false;
      }

      boolean readAllowed = fileCanRead(userUid, userGid, meta);;
      boolean executeAllowed = fileCanExecute(userUid, userGid, meta);;

    _logPermisions.debug("dirCanRead() read allowed :"+readAllowed+
     "  exec allowed :"+executeAllowed);

      if( !(readAllowed && executeAllowed) ) {
         _logPermisions.error(" read is not allowed ");
          return false;
      }

     return readAllowed && executeAllowed;
  }

  /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#worldCanRead(java.lang.String)
 */
public boolean worldCanRead(String pnfsPath) throws CacheException
  {
    _logPermisions.debug("worldCanRead("+pnfsPath+")");
     FsPath parent_path = new FsPath(pnfsPath);
     // go one level up
     parent_path.add("..");

     String parent = parent_path.toString();
     FileMetaData meta = _metaDataSource.getMetaData(parent);
    _logPermisions.debug("worldCanRead() parent meta = "+meta);
      if( !meta.isDirectory() ) {
         _logPermisions.error(parent +" exists and is not a directory, can not read "+pnfsPath);
          return false;
      }

      FileMetaData.Permissions world = meta.getWorldPermissions() ;
      boolean parentReadAllowed =      world.canRead() ;
      boolean parentExecuteAllowed =   world.canExecute() ;
    _logPermisions.debug("worldCanRead() parent read allowed :"+parentReadAllowed+
     " parent exec allowed :"+parentExecuteAllowed);

      if(!parentReadAllowed || ! parentExecuteAllowed) {
         _logPermisions.error(" parent read is not allowed ");
          return false;
      }

      meta = _metaDataSource.getMetaData(pnfsPath);
     _logPermisions.debug("worldCanRead() file meta = "+meta);
      world = meta.getWorldPermissions() ;
      boolean readAllowed = world.canRead() ;

    _logPermisions.debug("worldCanRead() file read allowed :"+readAllowed);
      return readAllowed;
  }

  /* (non-Javadoc)
 * @see diskCacheV111.services.PermissionHandlerInterface#worldCanWrite(java.lang.String)
 */
public boolean worldCanWrite(String pnfsPath) throws CacheException  {
      //simple and elegant
      return false;
  }

  //////////////////////////////////////////////////////////////////////////////////
  ///
  ///  Low level checks
  ///
  /////////////////////////////////////////////////////////////////////////////////


  private static boolean fileCanWrite( int userUid, int[] userGid, FileMetaData meta) {

      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;

      boolean writeAllowed = false;

      if( meta.getUid() == userUid ) {
          writeAllowed = user.canWrite();
      } else if (meta.getGid() == userGid[0] ) {
          writeAllowed = group.canWrite();
      }else {
          // world = all except user and group
          writeAllowed = world.canWrite() ;
      }

      return writeAllowed;
  }


  private static boolean fileCanExecute( int userUid, int[] userGid, FileMetaData meta) {

      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;

      boolean writeAllowed = false;

      if( meta.getUid() == userUid ) {
          writeAllowed = user.canExecute();
      } else if (meta.getGid() == userGid[0]) {
          writeAllowed = group.canExecute();
      }else {
          // world = all except user and group
          writeAllowed = world.canExecute() ;
      }

      return writeAllowed;
  }

  private static boolean fileCanRead( int userUid, int[] userGid, FileMetaData meta) {

      FileMetaData.Permissions user  = meta.getUserPermissions() ;
      FileMetaData.Permissions group = meta.getGroupPermissions();
      FileMetaData.Permissions world = meta.getWorldPermissions() ;

      boolean readAllowed = false;

      if( meta.getUid() == userUid ) {
          readAllowed = user.canRead();
      } else if (meta.getGid() == userGid[0] ) {
          readAllowed = group.canRead();
      }else {
          // world = all except user and group
          readAllowed = world.canRead() ;
      }

      return readAllowed;
  }


}
