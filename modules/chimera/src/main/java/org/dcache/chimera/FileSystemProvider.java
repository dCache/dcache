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

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.acl.ACE;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.util.Checksum;

public interface FileSystemProvider extends Closeable {

    public abstract FsInode createLink(String src, String dest)
            throws ChimeraFsException;

    public abstract FsInode createLink(FsInode parent, String name, String dest)
            throws ChimeraFsException;

    public abstract FsInode createLink(FsInode parent, String name, int uid,
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
    public abstract FsInode createHLink(FsInode parent, FsInode inode,
            String name) throws ChimeraFsException;

    public abstract FsInode createFile(String path) throws ChimeraFsException;

    public abstract FsInode createFile(FsInode parent, String name)
            throws ChimeraFsException;

    public abstract FsInode createFileLevel(FsInode inode, int level)
            throws ChimeraFsException;

    public abstract FsInode createFile(FsInode parent, String name, int owner,
            int group, int mode) throws ChimeraFsException;

    public abstract FsInode createFile(FsInode parent, String name, int owner,
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
    public abstract void createFileWithId(FsInode parent, FsInode inode,
            String name, int owner, int group, int mode, int type)
            throws ChimeraFsException;

    public abstract DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream(FsInode dir)
            throws ChimeraFsException;

    public abstract void remove(String path) throws ChimeraFsException;

    public abstract void remove(FsInode parent, String name)
            throws ChimeraFsException;

    public abstract void remove(FsInode inode) throws ChimeraFsException;

    public abstract Stat stat(String path)
            throws ChimeraFsException;

    public abstract Stat stat(FsInode inode) throws ChimeraFsException;

    public abstract Stat stat(FsInode inode, int level)
            throws ChimeraFsException;

    public abstract FsInode mkdir(String path) throws ChimeraFsException;

    public abstract FsInode mkdir(FsInode parent, String name)
            throws ChimeraFsException;

    public abstract FsInode mkdir(FsInode parent, String name, int owner,
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
     * @param tags Tags to set on new directory
     * @return Inode of newly created directory
     * @throws ChimeraFsException
     */
    FsInode mkdir(FsInode parent, String name, int owner, int group, int mode, Map<String,byte[]> tags)
            throws ChimeraFsException;

    public abstract FsInode path2inode(String path) throws ChimeraFsException;

    public abstract FsInode path2inode(String path, FsInode startFrom)
            throws ChimeraFsException;

    public abstract List<FsInode> path2inodes(String path)
        throws ChimeraFsException;

    public abstract List<FsInode> path2inodes(String path, FsInode startFrom)
        throws ChimeraFsException;

    public abstract FsInode inodeOf(FsInode parent, String name)
            throws ChimeraFsException;

    public abstract String inode2path(FsInode inode) throws ChimeraFsException;

    /**
     *
     * @param inode
     * @param startFrom
     * @return path of inode starting from startFrom
     * @throws ChimeraFsException
     */
    public abstract String inode2path(FsInode inode, FsInode startFrom,
            boolean inclusive) throws ChimeraFsException;

    public abstract boolean isIoEnabled(FsInode inode)
            throws ChimeraFsException;

    public abstract boolean removeFileMetadata(String path, int level)
            throws ChimeraFsException;

    public abstract FsInode getParentOf(FsInode inode)
            throws ChimeraFsException;

    public abstract void setFileSize(FsInode inode, long newSize)
            throws ChimeraFsException;

    public abstract void setFileOwner(FsInode inode, int newOwner)
            throws ChimeraFsException;

    public abstract void setFileOwner(FsInode inode, int level, int newOwner)
            throws ChimeraFsException;

    public abstract void setFileName(FsInode dir, String oldName, String newName)
            throws ChimeraFsException;

    public abstract void setInodeAttributes(FsInode inode, int level, Stat stat)
            throws ChimeraFsException;

    public abstract void setFileATime(FsInode inode, long atime)
            throws ChimeraFsException;

    public abstract void setFileATime(FsInode inode, int level, long atime)
            throws ChimeraFsException;

    public abstract void setFileCTime(FsInode inode, long ctime)
            throws ChimeraFsException;

    public abstract void setFileCTime(FsInode inode, int level, long ctime)
            throws ChimeraFsException;

    public abstract void setFileMTime(FsInode inode, long mtime)
            throws ChimeraFsException;

    public abstract void setFileMTime(FsInode inode, int level, long mtime)
            throws ChimeraFsException;

    public abstract void setFileGroup(FsInode inode, int newGroup)
            throws ChimeraFsException;

    public abstract void setFileGroup(FsInode inode, int level, int newGroup)
            throws ChimeraFsException;

    public abstract void setFileMode(FsInode inode, int newMode)
            throws ChimeraFsException;

    public abstract void setFileMode(FsInode inode, int level, int newMode)
            throws ChimeraFsException;

    public abstract void setInodeIo(FsInode inode, boolean enable)
            throws ChimeraFsException;

    public abstract int write(FsInode inode, int level, long beginIndex, byte[] data,
            int offset, int len) throws ChimeraFsException;

    public abstract int read(FsInode inode, int level, long beginIndex, byte[] data,
            int offset, int len) throws ChimeraFsException;

    public abstract byte[] readLink(String path) throws ChimeraFsException;

    public abstract byte[] readLink(FsInode inode) throws ChimeraFsException;

    public abstract boolean move(String source, String dest);

    /**
     * Move filesystem object from one directory into an other. If {@code source}
     * and {@code dest} both refer to the  same  existing  file, the move performs no action.
     * If destination object exists, then source object must be the same type.
     *
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
    public abstract boolean move(FsInode srcDir, String source,
            FsInode destDir, String dest) throws ChimeraFsException;

    public abstract List<StorageLocatable> getInodeLocations(FsInode inode,
            int type) throws ChimeraFsException;

    public abstract List<StorageLocatable> getInodeLocations(FsInode inode)
            throws ChimeraFsException;

    public abstract void addInodeLocation(FsInode inode, int type,
            String location) throws ChimeraFsException;

    public abstract void clearInodeLocation(FsInode inode, int type,
            String location) throws ChimeraFsException;

    public abstract String[] tags(FsInode inode) throws ChimeraFsException;

    Map<String, byte[]> getAllTags(FsInode inode) throws ChimeraFsException;

    public abstract void createTag(FsInode inode, String name)
            throws ChimeraFsException;

    public abstract void createTag(FsInode inode, String name, int uid,
            int gid, int mode) throws ChimeraFsException;

    public abstract int setTag(FsInode inode, String tagName, byte[] data,
            int offset, int len) throws ChimeraFsException;

    public abstract void removeTag(FsInode dir, String tagName)
            throws ChimeraFsException;

    public abstract void removeTag(FsInode dir) throws ChimeraFsException;

    public abstract int getTag(FsInode inode, String tagName, byte[] data,
            int offset, int len) throws ChimeraFsException;

    public abstract Stat statTag(FsInode dir, String name)
            throws ChimeraFsException;

    public void setTagOwner(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException;

    public void setTagOwnerGroup(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException;

    public void setTagMode(FsInode_TAG tagInode, String name, int mode) throws ChimeraFsException;

    public abstract int getFsId();

    public abstract void setStorageInfo(FsInode inode,
            InodeStorageInformation storageInfo) throws ChimeraFsException;

    /**
     *
     * @param inode
     * @param accessLatency
     * @throws ChimeraFsException
     */
    public abstract void setAccessLatency(FsInode inode,
            AccessLatency accessLatency) throws ChimeraFsException;

    public abstract void setRetentionPolicy(FsInode inode,
            RetentionPolicy retentionPolicy) throws ChimeraFsException;

    public abstract InodeStorageInformation getStorageInfo(FsInode inode)
            throws ChimeraFsException;

    public abstract AccessLatency getAccessLatency(FsInode inode)
            throws ChimeraFsException;

    public abstract RetentionPolicy getRetentionPolicy(FsInode inode)
            throws ChimeraFsException;

    public abstract void setInodeChecksum(FsInode inode, int type,
            String checksum) throws ChimeraFsException;

    public abstract void removeInodeChecksum(FsInode inode, int type)
            throws ChimeraFsException;

    public abstract Set<Checksum> getInodeChecksums(FsInode inode)
                    throws ChimeraFsException;

    public abstract String getInfo();

    /**
     * Get file system statistic.
     *
     * @return {@link FsStat} of the file system
     */
    public abstract FsStat getFsStat() throws ChimeraFsException;

    /**
     * Get list of Access Control Entries for specified inode.
     * @param inode
     * @return ordered list of {@link ACE}.
     * @throws ChimeraFsException
     */
    public abstract List<ACE> getACL(FsInode inode) throws ChimeraFsException;

    /**
     * Set Access Control Entries list for specified inode.
     * @param inode
     * @param acl
     * @throws ChimeraFsException
     */
    public abstract void setACL(FsInode inode, List<ACE> acl) throws ChimeraFsException;

    /**
     * Get a {code FsInode} corresponding to provided bytes.
     * @param bytes
     * @return
     * @throws ChimeraFsException
     */
    public FsInode inodeFromBytes(byte[] bytes) throws ChimeraFsException;

    /**
     * Get a bytes corresponding to provided {code FsInode} into.
     * @param inode
     * @return
     * @throws ChimeraFsException
     */
    public byte[] inodeToBytes(FsInode inode) throws ChimeraFsException;

    /**
     * Query the PoolManager for live locality information.
     * @param node
     * @return
     * @throws ChimeraFsException
     */
    public String getFileLocality(FsInode_PLOC node) throws ChimeraFsException;

    /**
     * Implementation-specific.  Can be NOP.
     *
     * @param pnfsid
     * @param lifetime
     * @throws ChimeraFsException
     */
    public void pin(String pnfsid, long lifetime) throws ChimeraFsException;

    /**
     * Implementation-specific.  Can be NOP.
     *
     * @param pnfsid
     * @throws ChimeraFsException
     */
    public void unpin(String pnfsid) throws ChimeraFsException;
}
