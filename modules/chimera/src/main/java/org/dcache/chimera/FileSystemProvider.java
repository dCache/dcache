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
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.dcache.acl.ACE;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.util.Checksum;

public interface FileSystemProvider extends Closeable {

    /**
     * The different possible states of a pin.
     */
    public enum PinState {
        /**
         * Pin request accepted, but not yet fulfilled.
         */
        PINNING,

        /**
         * Pin established.
         */
        PINNED,

        /**
         * Pin is to be removed.
         */
        UNPINNING;
    }

    /**
     * Information about an individual pin.
     */
    public interface PinInfo {

        /**
         * dCache internal identifier for this pin.
         */
        long getId();

        /**
         * When the pin was created.
         */
        Instant getCreationTime();

        /**
         * When the pin will be removed, if at all.
         */
        Optional<Instant> getExpirationTime();

        /**
         * The current status of this pin.
         */
        PinState getState();

        /**
         * A door/user-supplied identifier for this pin.
         */
        Optional<String> getRequestId();

        /**
         * Whether the requesting user can unpin this pin.
         */
        boolean isUnpinnable();
    }

    FsInode createLink(String src, String dest)
          throws ChimeraFsException;

    FsInode createLink(FsInode parent, String name, String dest)
          throws ChimeraFsException;

    FsInode createLink(FsInode parent, String name, int uid,
          int gid, int mode, byte[] dest) throws ChimeraFsException;

    /**
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
     * @param id
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @throws ChimeraFsException
     */
    void createFileWithId(FsInode parent, String id, String name, int owner, int group, int mode,
          int type)
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
     * @param name      Name of the entry to remove.
     * @param inode     Inode of the entry to remove.
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
     * <p>
     * In contrast to the other mkdir calls, the new directory does not inherit the parent tags.
     * Instead the new directory is initialized with {@code tags}.
     *
     * @param parent Inode of parent directory
     * @param name   Name of new directory
     * @param owner  UID of owner
     * @param group  GID of group
     * @param mode   Permissions
     * @param acl    ACL to set on new directory
     * @param tags   Tags to set on new directory
     * @return Inode of newly created directory
     * @throws ChimeraFsException
     */
    FsInode mkdir(FsInode parent, String name, int owner, int group, int mode, List<ACE> acl,
          Map<String, byte[]> tags)
          throws ChimeraFsException;

    FsInode path2inode(String path) throws ChimeraFsException;

    FsInode path2inode(String path, FsInode startFrom)
          throws ChimeraFsException;

    /**
     * Maps an inode to a persistent identifier.
     * <p>
     * May throw FileNotFoundHimeraFsException if the inode does not exist, however since the
     * mapping is semi persistent (it may only change while Chimera/dCache is shut down), it may be
     * cached and there is no guarantee that the inode exists if this method does not throw an
     * exception.
     *
     * @param inode
     * @return
     * @throws ChimeraFsException
     */
    String inode2id(FsInode inode) throws ChimeraFsException;

    /**
     * Maps a persistent identifier to an inode.
     * <p>
     * May throw FileNotFoundHimeraFsException if such an inode does not exist, however since the
     * mapping is semi persistent (it may only change while Chimera/dCache is shut down), it may be
     * cached and there is no guarantee that the inode exists if this method does not throw an
     * exception.
     * <p>
     * If {@code stat} is [@code STAT}, the stat cache of the inode is pre-filled. The stat
     * information is up to date as of this call.
     *
     * @param id
     * @return
     * @throws ChimeraFsException
     */
    FsInode id2inode(String id, StatCacheOption stat) throws ChimeraFsException;

    List<FsInode> path2inodes(String path)
          throws ChimeraFsException;

    List<FsInode> path2inodes(String path, FsInode startFrom)
          throws ChimeraFsException;

    FsInode inodeOf(FsInode parent, String name, StatCacheOption stat)
          throws ChimeraFsException;

    String inode2path(FsInode inode) throws ChimeraFsException;

    /**
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

    /**
     * Locate the given inode within the namespace.  For directories, there is (at most) a single
     * location, but files may yield multiple locations if the file has hard links.
     *
     * @param inode the target to locate
     * @return a collection of locations where the file may be found.
     * @throws ChimeraFsException
     */
    Collection<Link> find(FsInode inode) throws ChimeraFsException;

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
     * Change the name of a file system object. If {@code source} and {@code dest} both refer to the
     *  same  existing  file, the move performs no action. If destination object exists, then source
     * object must be the same type and will overwrite the destination.
     *
     * @param inode   inode of the file to rename
     * @param srcDir  inode of the source directory
     * @param source  name of the file in srcDir
     * @param destDir inode of the destination directory
     * @param dest    name of the new file in destDir
     * @return true it underlying filesystem has been changed.
     * @throws FileNotFoundHimeraFsException if source file does not exists
     * @throws FileExistsChimeraFsException  if destination exists and it not the same type as
     *                                       source
     * @throws DirNotEmptyHimeraFsException  if destination exists, is a directory and not empty
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

    void clearTapeLocations(FsInode inode) throws ChimeraFsException;

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

    /**
     * Update all subdirectories of {@code inode} with a given tag. The tag should exist in the
     * specified directory. It's not required that directory is the origin of the tag.
     *
     * @param inode   The inode of the directory.
     * @param tagName The name of the tag.
     * @return number of updated subdirectories.
     * @throws ChimeraFsException if there is any problem
     */
    public int pushTag(FsInode inode, String tagName) throws ChimeraFsException;

    /**
     * Find a list of origin tags that have the supplied name.  Origin tags are those tags
     * explicitly created in the namespace.  Tags that are inherited automatically are not origin
     * tags and are not include in the response.
     *
     * @param tagName The name of the origin tag.
     * @return a list of origin tags
     * @throws ChimeraFsException if there is any problem
     */
    List<OriginTag> findTags(String tagName) throws ChimeraFsException;

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
     *
     * @param inode
     * @return ordered list of {@link ACE}.
     * @throws ChimeraFsException
     */
    List<ACE> getACL(FsInode inode) throws ChimeraFsException;

    /**
     * Set Access Control Entries list for specified inode.
     *
     * @param inode
     * @param acl
     * @throws ChimeraFsException
     */
    void setACL(FsInode inode, List<ACE> acl) throws ChimeraFsException;

    /**
     * Query the PoolManager for live locality information.
     *
     * @param node
     * @return
     * @throws ChimeraFsException
     */
    String getFileLocality(FsInode_PLOC node) throws ChimeraFsException;

    /**
     * Implementation-specific.  Can be NOP.
     *
     * @param inode
     * @param lifetime
     * @throws ChimeraFsException
     */
    void pin(FsInode inode, long lifetime) throws ChimeraFsException;

    /**
     * Implementation-specific.  Can be NOP.
     *
     * @param inode
     * @throws ChimeraFsException
     */
    void unpin(FsInode inode) throws ChimeraFsException;

    List<PinInfo> listPins(FsInode inode) throws ChimeraFsException;

    enum StatCacheOption {
        STAT, NO_STAT
    }

    /**
     * Mode of setXattr operation.
     */
    enum SetXattrMode {

        /**
         * Created if the named attribute does not already exist, or the value will be replaced if
         * the attribute already exists.
         */
        EITHER,

        /**
         * Perform a pure create, which fails if the named attribute exists already.
         */
        CREATE,
        /**
         * Perform a pure replace operation, which fails if the named attribute does not already
         * exist.
         */
        REPLACE
    }

    /**
     * Get an Extended Attribute of a inode.
     *
     * @param inode file system object.
     * @param attr  extended attribute name.
     * @return value of the attribute.
     * @throws ChimeraFsException
     */
    byte[] getXattr(FsInode inode, String attr) throws ChimeraFsException;

    /**
     * Set or change extended attribute of a given file system object.
     *
     * @param inode file system object.
     * @param attr  extended attribute name.
     * @param value of the attribute.
     * @param mode  mode of setXattr operation.
     * @throws ChimeraFsException
     */
    void setXattr(FsInode inode, String attr, byte[] value, SetXattrMode mode)
          throws ChimeraFsException;

    /**
     * Retrieve an array of extended attribute names for a given file system object.
     *
     * @param inode file system object.
     * @return a set of extended attribute names.
     * @throws ChimeraFsException
     */
    Set<String> listXattrs(FsInode inode) throws ChimeraFsException;

    /**
     * Remove specified extended attribute for a given file system object.
     *
     * @param inode file system object.
     * @param attr  extended attribute name.
     * @throws ChimeraFsException
     */
    void removeXattr(FsInode inode, String attr) throws ChimeraFsException;

}
