package diskCacheV111.services;

import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;

public interface PermissionHandlerInterface {

	/**
	 *
	 * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @return true if user allowed to read the file
	 * @throws CacheException 
	 */
	public abstract boolean canReadFile(Subject subject, String pnfsPath, Origin origin)
			throws CacheException;
	
	
    /**
     * @param subject
     * @param pnfsPath
     * @param userOrigin
     * @return true if user allowed to write into the file
     * @throws CacheException
     */
	public abstract boolean canWriteFile(Subject subject, String pnfsPath, Origin origin)
			throws CacheException;
	
	
	/**
	 *
	 * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @return true if user allowed to create a directory
	 * @throws CacheException
	 */
	public abstract boolean canCreateDir(Subject subject,
			String pnfsPath, Origin origin) throws CacheException;

	/**
	 *
	 * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @return true if user allowed to remove the directory
	 * @throws CacheException
	 */
	public abstract boolean canDeleteDir(Subject subject,
			String pnfsPath, Origin origin) throws CacheException;


	/**
	 *
	 * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @return true if user allowed to create a file in the given directory
	 * @throws CacheException
	 */
	public abstract boolean canCreateFile(Subject subject, String pnfsPath, Origin origin)
			throws CacheException;

	/**
	 *
	 * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @return true if user allowed remove the file
	 * @throws CacheException
	 */
	public abstract boolean canDeleteFile(Subject subject, String pnfsPath, Origin origin)
			throws CacheException;
	
	/**
	 *
     * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @return true if user allowed to list the directory
	 * @throws CacheException
	 */
	public abstract boolean canListDir(Subject subject, String pnfsPath, Origin origin)
			throws CacheException;
	
	/**
	 *
	 * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @param attribute
	 * @return true if user allowed to set attributes
	 * @throws CacheException
	 */
	public abstract boolean canSetAttributes(Subject subject, String pnfsPath, Origin userOrigin, FileAttribute attribute)
	       throws CacheException;
	
	/**
	 *
	 * @param subject
	 * @param pnfsPath
	 * @param userOrigin
	 * @param attribute
	 * @return true if user allowed to get attributes
	 * @throws CacheException
	 */
	public abstract boolean canGetAttributes(Subject subject, String pnfsPath, Origin userOrigin, FileAttribute attribute)
    throws CacheException;
	
	
}