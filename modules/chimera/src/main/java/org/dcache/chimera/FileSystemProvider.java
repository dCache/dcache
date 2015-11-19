/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.acl.ACE;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.util.Checksum;

public interface FileSystemProvider extends Closeable {

    FsInode createLink(String src, String dest)
            throws ChimeraFsException;

    FsInode createLink(FsInode parent, String name, String dest)
            throws ChimeraFsException;

    FsInode createLink(FsInode parent, String name, int uid,
                       int gid, int mode, byte[] dest) throws ChimeraFsException;

    /**
     *
     * create a hard link
     *
     * @param parent inode of directory where to create
     * @param inode
     * @param name
     * @return
     * @throws ChimeraFsException
     */
    FsInode createHLink(FsInode parent, FsInode inode,
                        String name) throws ChimeraFsException;

    FsInode createFile(String path) throws ChimeraFsException;

    FsInode createFile(FsInode parent, String name)
            throws ChimeraFsException;

    FsInode createFileLevel(FsInode inode, int level)
            throws ChimeraFsException;

    FsInode createFile(FsInode parent, String name, int owner,
                       int group, int mode) throws ChimeraFsException;

    FsInode createFile(FsInode parent, String name, int owner,
                       int group, int mode, int type) throws ChimeraFsException;

    /**
     * Create a new entry with given inode id.
     *
     * @param parent
     * @param inode
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @throws ChimeraFsException
     */
    void createFileWithId(FsInode parent, FsInode inode,
                          String name, int owner, int group, int mode, int type)
            throws ChimeraFsException;

    String[] listDir(String dir);

    String[] listDir(FsInode dir) throws ChimeraFsException;

    DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream(FsInode dir)
            throws ChimeraFsException;

    void remove(String path) throws ChimeraFsException;

    /**
     * Removes a directory entry.
     *
     * @param directory Inode of the directory.
     * @param name Name of the entry to remove.
     * @param inode Inode of the entry to remove.
     *
     * @throws ChimeraFsException
     */
    void remove(FsInode directory, String name, FsInode inode)
            throws ChimeraFsException;

    void remove(FsInode inode) throws ChimeraFsException;

    Stat stat(String path)
            throws ChimeraFsException;

    Stat stat(FsInode inode) throws ChimeraFsException;

    Stat stat(FsInode inode, int level)
            throws ChimeraFsException;

    FsInode mkdir(String path) throws ChimeraFsException;

    FsInode mkdir(FsInode parent, String name)
            throws ChimeraFsException;

    FsInode mkdir(FsInode parent, String name, int owner,
                  int group, int mode) throws ChimeraFsException;

    /**
     * Create a new directory.
     *
     * In contrast to the other mkdir calls, the new directory does not inherit
     * the parent tags. Instead the new directory is initialized with {@code tags}.
     *
     * @param parent Inode of parent directory
     * @param name Name of new directory
     * @param owner UID of owner
     * @param group GID of group
     * @param mode Permissions
     * @param acl ACL to set on new directory
     * @param tags Tags to set on new directory
     * @return Inode of newly created directory
     * @throws ChimeraFsException
     */
    FsInode mkdir(FsInode parent, String name, int owner, int group, int mode, List<ACE> acl, Map<String, byte[]> tags)
            throws ChimeraFsException;

    FsInode path2inode(String path) throws ChimeraFsException;

    FsInode path2inode(String path, FsInode startFrom)
            throws ChimeraFsException;

    List<FsInode> path2inodes(String path)
        throws ChimeraFsException;

    List<FsInode> path2inodes(String path, FsInode startFrom)
        throws ChimeraFsException;

    FsInode inodeOf(FsInode parent, String name)
            throws ChimeraFsException;

    String inode2path(FsInode inode) throws ChimeraFsException;

    /**
     *
     * @param inode
     * @param startFrom
     * @return path of inode starting from startFrom
     * @throws ChimeraFsException
     */
    String inode2path(FsInode inode, FsInode startFrom) throws ChimeraFsException;

    boolean isIoEnabled(FsInode inode)
            throws ChimeraFsException;

    boolean removeFileMetadata(String path, int level)
            throws ChimeraFsException;

    FsInode getParentOf(FsInode inode)
            throws ChimeraFsException;

    void setInodeAttributes(FsInode inode, int level, Stat stat)
            throws ChimeraFsException;

    void setInodeIo(FsInode inode, boolean enable)
            throws ChimeraFsException;

    int write(FsInode inode, int level, long beginIndex, byte[] data,
              int offset, int len) throws ChimeraFsException;

    int read(FsInode inode, int level, long beginIndex, byte[] data,
             int offset, int len) throws ChimeraFsException;

    byte[] readLink(String path) throws ChimeraFsException;

    byte[] readLink(FsInode inode) throws ChimeraFsException;

    /**
     * Change the name of a file system object. If {@code source} and {@code dest} both
     * refer to the  same  existing  file, the move performs no action. If destination
     * object exists, then source object must be the same type and will overwrite the
     * destination.
     *
     * @param inode inode of the file to rename
     * @param srcDir inode of the source directory
     * @param source name of the file in srcDir
     * @param destDir inode of the destination directory
     * @param dest name of the new file in destDir
     * @return true it underlying filesystem has been changed.
     * @throws FileNotFoundHimeraFsException if source file does not exists
     * @throws FileExistsChimeraFsException if destination exists and it not the
     *	    same type as source
     * @throws DirNotEmptyHimeraFsException if destination exists, is a directory
     *	    and not empty
     */
    boolean rename(FsInode inode, FsInode srcDir, String source,
                   FsInode destDir, String dest) throws ChimeraFsException;

    List<StorageLocatable> getInodeLocations(FsInode inode,
                                             int type) throws ChimeraFsException;

    List<StorageLocatable> getInodeLocations(FsInode inode)
            throws ChimeraFsException;

    void addInodeLocation(FsInode inode, int type,
                          String location) throws ChimeraFsException;

    void clearInodeLocation(FsInode inode, int type,
                            String location) throws ChimeraFsException;

    String[] tags(FsInode inode) throws ChimeraFsException;

    Map<String, byte[]> getAllTags(FsInode inode) throws ChimeraFsException;

    void createTag(FsInode inode, String name)
            throws ChimeraFsException;

    void createTag(FsInode inode, String name, int uid,
                   int gid, int mode) throws ChimeraFsException;

    int setTag(FsInode inode, String tagName, byte[] data,
               int offset, int len) throws ChimeraFsException;

    void removeTag(FsInode dir, String tagName)
            throws ChimeraFsException;

    void removeTag(FsInode dir) throws ChimeraFsException;

    int getTag(FsInode inode, String tagName, byte[] data,
               int offset, int len) throws ChimeraFsException;

    Stat statTag(FsInode dir, String name)
            throws ChimeraFsException;

    void setTagOwner(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException;

    void setTagOwnerGroup(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException;

    void setTagMode(FsInode_TAG tagInode, String name, int mode) throws ChimeraFsException;

    int getFsId();

    void setStorageInfo(FsInode inode,
                        InodeStorageInformation storageInfo) throws ChimeraFsException;

    InodeStorageInformation getStorageInfo(FsInode inode)
            throws ChimeraFsException;

    void setInodeChecksum(FsInode inode, int type,
                          String checksum) throws ChimeraFsException;

    void removeInodeChecksum(FsInode inode, int type)
            throws ChimeraFsException;

    Set<Checksum> getInodeChecksums(FsInode inode)
                    throws ChimeraFsException;

    String getInfo();

    /**
     * Get file system statistic.
     *
     * @return {@link FsStat} of the file system
     */
    FsStat getFsStat() throws ChimeraFsException;

    /**
     * Get list of Access Control Entries for specified inode.
     * @param inode
     * @return ordered list of {@link ACE}.
     * @throws ChimeraFsException
     */
    List<ACE> getACL(FsInode inode) throws ChimeraFsException;

    /**
     * Set Access Control Entries list for specified inode.
     * @param inode
     * @param acl
     * @throws ChimeraFsException
     */
    void setACL(FsInode inode, List<ACE> acl) throws ChimeraFsException;

    /**
     * Get a {code FsInode} corresponding to provided bytes.
     * @param bytes
     * @return
     * @throws ChimeraFsException
     */
    FsInode inodeFromBytes(byte[] bytes) throws ChimeraFsException;

    /**
     * Get a bytes corresponding to provided {code FsInode} into.
     * @param inode
     * @return
     * @throws ChimeraFsException
     */
    byte[] inodeToBytes(FsInode inode) throws ChimeraFsException;

    /**
     * Query the PoolManager for live locality information.
     * @param node
     * @return
     * @throws ChimeraFsException
     */
    String getFileLocality(FsInode_PLOC node) throws ChimeraFsException;

    /**
     * Implementation-specific.  Can be NOP.
     *
     * @param pnfsid
     * @param lifetime
     * @throws ChimeraFsException
     */
    void pin(String pnfsid, long lifetime) throws ChimeraFsException;

    /**
     * Implementation-specific.  Can be NOP.
     *
     * @param pnfsid
     * @throws ChimeraFsException
     */
    void unpin(String pnfsid) throws ChimeraFsException;
}
