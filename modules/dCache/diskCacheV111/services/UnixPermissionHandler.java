// $Id: UnixPermissionHandler.java,v 1.2 2007-10-23 15:24:02 tigran Exp $

package diskCacheV111.services;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ConfigurationException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotFileCacheException;
import dmg.cells.nucleus.CellAdapter;

public class UnixPermissionHandler implements PermissionHandlerInterface {

    private CellAdapter _cell;
    private FileMetaDataSource _fileMetaDataSource;

    private final static Logger _logPermisions = Logger
            .getLogger("logger.org.dcache.authorization."
                    + UnixPermissionHandler.class.getName());

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#say(java.lang.String)
     */
    public UnixPermissionHandler(CellAdapter cell) 
        throws ConfigurationException
    {
        _cell = cell;
        
        String metaDataProvider =
            parseOption("meta-data-provider",
                        "diskCacheV111.services.PnfsManagerFileMetaDataSource");
    	try {
            _logPermisions.debug("Loading metaDataProvider :" + metaDataProvider);
            Class<?> [] argClass = { dmg.cells.nucleus.CellAdapter.class };
            Class<?> fileMetaDataSourceClass;
            
            fileMetaDataSourceClass = Class.forName(metaDataProvider);
            Constructor<?> fileMetaDataSourceCon = fileMetaDataSourceClass.getConstructor( argClass ) ;
            
            Object[] initargs = { _cell };
            _fileMetaDataSource = (FileMetaDataSource)fileMetaDataSourceCon.newInstance(initargs);
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException("Cannot instantiate " + metaDataProvider + ": " + e.getMessage(), e);
        } catch (InstantiationException e) {
            throw new ConfigurationException("Cannot instantiate " + metaDataProvider + ": " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new ConfigurationException("Cannot instantiate " + metaDataProvider + ": " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Cannot instantiate " + metaDataProvider + ": " + e.getMessage(), e);
        } catch (InvocationTargetException e) {
            throw new ConfigurationException("Cannot instantiate " + metaDataProvider + ": " + e.getTargetException().getMessage(), e);
        } catch (NoSuchMethodException e) {
            throw new ConfigurationException("Cannot instantiate " + metaDataProvider + ": " + e.getMessage(), e);
        }
    }

 

	public boolean canWriteFile(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
        
        boolean isAllowed = false;
        try {
            return fileCanWrite(subject, _fileMetaDataSource.getMetaData(pnfsPath));
        } catch (CacheException ce) {
            // file do not exist, check directory
        }

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _fileMetaDataSource.getMetaData(parent);
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not create "
                    + pnfsPath);
            return false;
        }

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);

        isAllowed = parentWriteAllowed && parentExecuteAllowed;
        
        _logPermisions.debug("canWriteFile(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + "): " +isAllowed);
        
        return isAllowed;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canCreateDir(int,
     *      int, java.lang.String)
     */
    public boolean canCreateDir(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
    	
    	boolean isAllowed = false;
    	
        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _fileMetaDataSource.getMetaData(parent);
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not create "
                    + pnfsPath);
            return false;
        }

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);
        isAllowed = parentWriteAllowed && parentExecuteAllowed;
        
        _logPermisions.debug("canCreateDir(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + "): " + isAllowed);
        
        return isAllowed;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canDeleteDir(int,
     *      int, java.lang.String)
     */
    public boolean canDeleteDir(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
    	
    	boolean isAllowed = false;

        FileMetaData meta = _fileMetaDataSource.getMetaData(pnfsPath);
        if (!meta.isDirectory()) {
            _logPermisions.error("in canDeleteDir() pnfsPath: "+pnfsPath + " is not a directory");
            throw new CacheException("path is not a directory");
        }

        isAllowed = fileCanWrite(subject, meta);
        
        _logPermisions.debug("canDeleteDir(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + "): " +isAllowed);
        
        return isAllowed;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canDelete(int,
     *      int, java.lang.String)
     */
    public boolean canDeleteFile(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
        
        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _fileMetaDataSource.getMetaData(parent);
        _logPermisions.debug("canDeleteFile() parent meta = " + meta);
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not read "
                    + pnfsPath);
            return false;
        }

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);
        boolean parentReadAllowed = fileCanRead(subject, meta);

        _logPermisions.debug("canDeleteFile() parent read allowed :"
                + parentReadAllowed + " parent write allowed :"
                + parentWriteAllowed + " parent exec allowed :"
                + parentExecuteAllowed);

        if (!parentReadAllowed || !parentExecuteAllowed || !parentWriteAllowed) {
            _logPermisions.error(" parent write is not allowed ");
            return false;
        }

        meta = _fileMetaDataSource.getMetaData(pnfsPath);
        _logPermisions.debug("canDeleteFile() file meta = " + meta);

        boolean deleteAllowed = fileCanWrite(subject, meta);

        if (deleteAllowed) {
            _logPermisions.error("WARNING: canDeleteFile() delete of file "
                    + pnfsPath + " by user uid=" + subject.getUid() + " gid="
                    + subject.getGid() + " is allowed!");
        } else {
            _logPermisions.debug("canDeleteFile() delete of file " + pnfsPath
                    + " by user uid=" + subject.getUid() + " gid="
                    + subject.getGid() + " is not allowed");
        }
        
        _logPermisions.debug("canDeleteFile(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + "): " + deleteAllowed);
        
        return deleteAllowed;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canRead(int, int,
     *      java.lang.String)
     */
    public boolean canReadFile(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
    	
    	boolean isAllowed = false;
        
    	FileMetaData meta = _fileMetaDataSource.getMetaData(pnfsPath);
    	
    	if( !meta.isRegularFile() ) {
    		throw new NotFileCacheException(pnfsPath + " exists and not a regular file.");
    	}
        if (!fileCanRead(subject, meta)) {
            return false;
        }

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");
        String parent = parent_path.toString();

        isAllowed = dirCanRead(subject, parent);
        
        _logPermisions.debug("canReadFile(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + "): " + isAllowed);
        
        return isAllowed;
    }

    private boolean dirCanRead(Subject subject, String path) throws CacheException {
    	
    	boolean isAllowed = false;
        FileMetaData meta = _fileMetaDataSource.getMetaData(path);
        _logPermisions.debug("dirCanRead() meta = " + meta);
        if (!meta.isDirectory()) {
            _logPermisions.error(path
                    + " exists and is not a directory, can not read ");
            return false;
        }

        boolean readAllowed = fileCanRead(subject, meta);
        
        boolean executeAllowed = fileCanExecute(subject, meta);
        
        if (!(readAllowed && executeAllowed)) {
            _logPermisions.error(" read is not allowed ");
            return false;
        }
        
        isAllowed = readAllowed && executeAllowed;
        
        _logPermisions.debug("dirCanRead() read allowed :" + readAllowed
                + "  exec allowed :" + executeAllowed + ", dirCanRead: " + isAllowed);

        return isAllowed;
    }

    public boolean canCreateFile(Subject subject, String pnfsPath, Origin origin) throws CacheException {
    	
    	boolean isAllowed = false;

        try {
            return fileCanWrite(subject, _fileMetaDataSource.getMetaData(pnfsPath));
        } catch (CacheException ce) {
            // file do not exist, check directory
        }

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _fileMetaDataSource.getMetaData(parent);
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not create "
                    + pnfsPath);
            return false;
        }

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);
        
        isAllowed = parentWriteAllowed && parentExecuteAllowed;
        
        _logPermisions.debug("canCreateFile(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ") :" + isAllowed);
        
        return isAllowed;
    }

    public boolean canListDir(Subject subject, String pnfsPath, Origin origin) throws CacheException {
    	
    	boolean isAllowed = false;
        
    	FileMetaData meta = _fileMetaDataSource.getMetaData(pnfsPath);
        _logPermisions.debug("pnfsPath() meta = " + meta);
        
        if (!meta.isDirectory()) {
            _logPermisions.error(pnfsPath
                    + " exists and is not a directory, can not read ");
            return false;
        }

        boolean readAllowed = fileCanRead(subject, meta);
        
        boolean executeAllowed = fileCanExecute(subject, meta);
       
        if (!(readAllowed && executeAllowed)) {
            _logPermisions.error(" readdir is not allowed ");
            return false;
        }
        
        isAllowed=readAllowed && executeAllowed;
        
        _logPermisions.debug("canListDir() read allowed :" + readAllowed
                + "  exec allowed :" + executeAllowed + ". List directory: "+ isAllowed );
        
        return isAllowed;
    }

    public boolean canSetAttributes(Subject subject, String pnfsPath,
            Origin userOrigin, FileAttribute attribute) throws CacheException {    	
    	
    	boolean isAllowed = false;
    	
    	
    	 FileMetaData meta = _fileMetaDataSource.getMetaData(pnfsPath);
    	     	
    	 if( attribute == FileAttribute.FATTR4_OWNER || attribute == FileAttribute.FATTR4_OWNER_GROUP) {
    		 isAllowed = (meta.getUid() == subject.getUid() || subject.getUid() == 0);
    	 }else{
    		 isAllowed = canWriteFile(subject, pnfsPath, userOrigin);
    	 }
    	
    	 _logPermisions.debug("canSetAttributes(" + subject.getUid() + ","
                 + subject.getGid() + "," + pnfsPath + ") : " + isAllowed);
    	 
        return isAllowed;
    }

    public boolean canGetAttributes(Subject subject, String pnfsPath,
            Origin userOrigin, FileAttribute attribute) throws CacheException {
    	
    	
    	
    	_logPermisions.debug("canGetAttributes(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ") :  true");
    	
        return true;
    }
    // ////////////////////////////////////////////////////////////////////////////////
    // /
    // / Low level checks
    // /
    // ///////////////////////////////////////////////////////////////////////////////

    private static boolean fileCanWrite(Subject subject, FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean writeAllowed = false;

        if (meta.getUid() == subject.getUid()) {
            writeAllowed = user.canWrite();
        } else if (meta.getGid() == subject.getGids()[0]) {
            writeAllowed = group.canWrite();
        } else {
            // world = all except user and group
            writeAllowed = world.canWrite();
        }

        return writeAllowed;
    }

    private static boolean fileCanExecute(Subject subject, FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean writeAllowed = false;

        if (meta.getUid() == subject.getUid()) {
            writeAllowed = user.canExecute();
        } else if (meta.getGid() == subject.getGids()[0]) {
            writeAllowed = group.canExecute();
        } else {
            // world = all except user and group
            writeAllowed = world.canExecute();
        }

        return writeAllowed;
    }

    private static boolean fileCanRead(Subject subject, FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean readAllowed = false;

        if (meta.getUid() == subject.getUid()) {
            readAllowed = user.canRead();
        } else if (meta.getGid() == subject.getGids()[0]) {
            readAllowed = group.canRead();
        } else {
            // world = all except user and group
            readAllowed = world.canRead();
        }

        return readAllowed;
    }

    /**
     * Returns the value of a named cell argument.
     *
     * @param name the name of the cell argument to return
     * @param def the value to return when <code>name</code> is
     *            not defined or cannot be parsed
     */
    private String parseOption(String name, String def)
    {
        String value;
        String tmp = _cell.getArgs().getOpt(name);
        if (tmp != null && tmp.length() > 0) {
            value = tmp;
        } else {
            value = def;
        }

        if (value != null) {
        	_logPermisions.debug(name + "=" + value);
        }

        return value;
    }

}
