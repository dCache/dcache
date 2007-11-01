package diskCacheV111.services;

import diskCacheV111.util.CacheException;

public interface PermissionHandlerInterface {

    /**
     * @param userUid
     * @param userGid
     * @param pnfsPath
     * @return true if user allowed to write into the file
     * @throws CacheException
     */
	public abstract boolean canWrite(int userUid, int[] userGid, String pnfsPath)
			throws CacheException;

	/**
	 *
	 * @param userUid
	 * @param userGid
	 * @param pnfsPath
	 * @return true if user allowed to create a directory
	 * @throws CacheException
	 */
	public abstract boolean canCreateDir(int userUid, int[] userGid,
			String pnfsPath) throws CacheException;

	/**
	 *
	 * @param userUid
	 * @param userGid
	 * @param pnfsPath
	 * @return true if user allowed to remove the directory
	 * @throws CacheException
	 */
	public abstract boolean canDeleteDir(int userUid, int[] userGid,
			String pnfsPath) throws CacheException;

	/**
	 *
	 * @param userUid
	 * @param userGid
	 * @param pnfsPath
	 * @return true if user allowed remove the file
	 * @throws CacheException
	 */
	public abstract boolean canDelete(int userUid, int[] userGid, String pnfsPath)
			throws CacheException;

	/**
	 *
	 * @param userUid
	 * @param userGid
	 * @param pnfsPath
	 * @return true if user allowed to read the file
	 * @throws CacheException
	 */
	public abstract boolean canRead(int userUid, int[] userGid, String pnfsPath)
			throws CacheException;

	/**
	 *
	 * @param pnfsPath
	 * @return true if anyone allowed to read the file
	 * @throws CacheException
	 */
	public abstract boolean worldCanRead(String pnfsPath) throws CacheException;

	/**
	 *
	 * @param pnfsPath
	 * @return true if anyone allowed to write the file
	 * @throws CacheException
	 */
	public abstract boolean worldCanWrite(String pnfsPath)
			throws CacheException;

}