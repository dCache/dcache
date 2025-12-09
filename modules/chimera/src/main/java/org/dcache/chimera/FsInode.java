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

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.STAT;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.dcache.acl.ACE;
import org.dcache.chimera.posix.Stat;

/**
 * inode representation
 */
public class FsInode {

    protected final long _ino;
    /**
     * type of inode
     */
    private final FsInodeType _type;
    protected final FileSystemProvider _fs;
    /**
     * inode level. Level zero associated with real file
     */
    private final int _level;
    /**
     * inode posix stat Object
     */
    private Stat _stat;
    /**
     * parent inode. In case of hard links, one of the possible parents.
     */
    private FsInode _parent;

    /**
     * Copy constructor.
     */
    public FsInode(FileSystemProvider fs, FsInode inode) {
        this(fs, inode.ino(), inode.type(), inode.getLevel(), inode.getStatCache());
    }

    /**
     * create a new inode in filesystem fs with given id and type
     *
     * @param fs   file system
     * @param ino  the new id
     * @param type inode type
     */
    public FsInode(FileSystemProvider fs, long ino, FsInodeType type) {
        this(fs, ino, type, 0);
    }

    /**
     * create a new inode of type 'inode' in the filesystem fs with given id
     *
     * @param fs  file system
     * @param ino inode id
     */
    public FsInode(FileSystemProvider fs, long ino) {
        this(fs, ino, 0);
    }

    /**
     * Create a new FsInode with given id and level,  type == INODE
     *
     * @param fs
     * @param ino
     * @param level
     */
    public FsInode(FileSystemProvider fs, long ino, int level) {
        this(fs, ino, FsInodeType.INODE, level);
    }

    /**
     * Create a new FsInode with given id, type and level
     *
     * @param fs
     * @param ino
     * @param type
     * @param level
     */
    public FsInode(FileSystemProvider fs, long ino, FsInodeType type, int level) {
        this(fs, ino, type, level, null);
    }

    public FsInode(FileSystemProvider fs, long ino, FsInodeType type, int level, Stat stat) {
        checkArgument(level >= 0 && level <= JdbcFs.LEVELS_NUMBER,
              "invalid level number: " + level);
        _ino = ino;
        _fs = fs;
        _level = level;
        _type = type;
        _stat = stat;
    }

    public long ino() {
        return _ino;
    }

    /**
     * Generates new inode id assigned to filesystem with fsId == 0
     *
     * @return new id
     */
    public static final String generateNewID() {
        return InodeId.newID(0);
    }

    /**
     * Generates new inode id assigned to filesystem referenced by <i>fsId</>
     *
     * @param fsId
     * @return new id
     */
    public static final String generateNewID(int fsId) {
        return InodeId.newID(fsId);
    }

    /**
     * @return inode's type, e.q.: inode, tag, id
     */
    public FsInodeType type() {
        return _type;
    }

    /**
     * A helper method to generate the base part of identifier.
     *
     * @param opaque inode specific data
     * @return byte array identifying the inode.
     */
    protected final byte[] byteBase(byte[] opaque) {
        /**
         * allocate array with a correct number of bytes:
         *    1 - fs is
         *    1 - inode type
         *    1 - number of bytes in ino, for compatibility
         *    8 - ino,
         *    1 - size of type specific data
         *    opaque.len - opaque data
         */
        byte[] bytes = new byte[1 + 1 + 1 + Long.BYTES + 1 + opaque.length];
        ByteBuffer b = ByteBuffer.wrap(bytes);
        b.put((byte) _fs.getFsId())
              .put((byte) _type.getType())
              .put((byte) Long.BYTES) // set the file id size to be compatible with old format
              .putLong(_ino);

        b.put((byte) opaque.length);
        b.put(opaque);

        return bytes;
    }

    /**
     * @return a byte[] representation of inode, including type and fsid
     */
    public byte[] getIdentifier() {
        return byteBase(new byte[]{(byte) (0x30 + _level)}); // 0x30 is ascii code for '0'
    }

    /**
     * @return a String representation of inode id only
     */
    @Override
    public String toString() {
        return String.valueOf(_ino);
    }

    /**
     * gets the actual stat information of the inode and updated the cached value See also
     * statCache()
     *
     * @return Stat
     * @throws FileNotFoundChimeraFsException
     */
    public Stat stat() throws ChimeraFsException {
        _stat = _fs.stat(this, _level);
        return _stat;
    }

    /**
     * gets the cached value of  stat information of the inode See also stat()
     *
     * @return Stat
     * @throws FileNotFoundChimeraFsException
     */
    public Stat statCache() throws ChimeraFsException {
        return (_stat == null) ? stat() : _stat;
    }

    public int write(long pos, byte[] data, int offset, int len) throws ChimeraFsException {
        int ret = _fs.write(this, _level, pos, data, offset, len);
        _stat = null;
        return ret;
    }

    public int read(long pos, byte[] data, int offset, int len) throws ChimeraFsException {
        return _fs.read(this, _level, pos, data, offset, len);
    }

    /**
     * crate a directory with name 'newDir' in current inode
     */
    public FsInode mkdir(String newDir) throws ChimeraFsException {
        FsInode dir = _fs.mkdir(this, newDir);
        _stat = null;
        return dir;
    }

    /**
     * crate a directory with name 'newDir' in current inode with different access rights
     */
    public FsInode mkdir(String name, int owner, int group, int mode) throws ChimeraFsException {
        FsInode dir = _fs.mkdir(this, name, owner, group, mode);
        _stat = null;
        return dir;
    }

    /**
     * crate a directory with name 'newDir' in current inode with different access rights
     */
    public FsInode mkdir(String name, int owner, int group, int mode, List<ACE> acl,
          Map<String, byte[]> tags)
          throws ChimeraFsException {
        FsInode dir = _fs.mkdir(this, name, owner, group, mode, acl, tags);
        _stat = null;
        return dir;
    }

    /**
     * get inode of file in the current directory with name 'name'
     */
    public FsInode inodeOf(String name, FileSystemProvider.StatCacheOption stat)
          throws ChimeraFsException {
        return _fs.inodeOf(this, name, stat);
    }

    /**
     * create new file in the current directory with name 'name'
     */
    public FsInode create(String name, int uid, int gid, int mode) throws ChimeraFsException {
        FsInode file = _fs.createFile(this, name, uid, gid, mode);
        _stat = null;
        return file;
    }

    /**
     * create new link to a specified file in the current directory with name 'name' ( FsInode
     * parent, String name , int uid, int gid, int mode, byte[] dest)
     */
    public FsInode createLink(String name, int uid, int gid, int mode, byte[] dest)
          throws ChimeraFsException {
        if (!this.isDirectory()) {
            throw new NotDirChimeraException(this);
        }
        FsInode link = _fs.createLink(this, name, uid, gid, mode, dest);
        _stat = null;
        return link;
    }

    /**
     * get inode of root element of the file system
     */
    public static FsInode getRoot(FileSystemProvider fs) throws ChimeraFsException {
        return fs.path2inode("/");
    }

    public boolean exists() throws ChimeraFsException {

        boolean rc = false;

        try {
            statCache();
            rc = true;
        } catch (FileNotFoundChimeraFsException hfe) {
        }
        return rc;
    }

    public boolean isDirectory() {

        boolean rc = false;

        try {
            if (exists() && ((_stat.getMode() & UnixPermission.F_TYPE) == UnixPermission.S_IFDIR)) {
                rc = true;
            }
        } catch (ChimeraFsException ignore) {
        }

        return rc;
    }

    public boolean isLink() {

        boolean rc = false;

        try {
            if (exists() && new UnixPermission(_stat.getMode()).isSymLink()) {
                rc = true;
            }
        } catch (ChimeraFsException ignore) {
        }

        return rc;
    }

    public byte[] readlink() throws ChimeraFsException {

        if (!isLink()) {
            throw new ChimeraFsException("not a link");
        }

        return _fs.readLink(this);
    }

    public void remove(String name) throws ChimeraFsException {

        if (!isDirectory()) {
            throw new ChimeraFsException("Not a directory");
        }

        _fs.remove(this, name, inodeOf(name, STAT));
        _stat = null;
    }

    public int fsId() {
        return _fs.getFsId();
    }

    public FileSystemProvider getFs() {
        return _fs;
    }

    public FsInode getParent() {
        if (_parent == null) {
            try {
                Collection<Link> locations = _fs.find(this);
                _parent = locations.isEmpty() ? null : locations.iterator().next().getParent();
            } catch (ChimeraFsException e) {
            }
        }

        return _parent;
    }

    public void setParent(FsInode parent) {
        _parent = parent;
    }

    public void setStat(Stat predefinedStat) throws ChimeraFsException {
        _fs.setInodeAttributes(this, _level, predefinedStat);
        if (_stat != null) {
            _stat.update(predefinedStat);
        }
    }

    protected Stat getStatCache() {
        return _stat;
    }

    protected void setStatCache(Stat predefinedStat) {
        _stat = predefinedStat;
    }

    //  for use in Collections
    // Override from Object
    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }
        if (!(o instanceof FsInode)) {
            return false;
        }

        FsInode otherInode = (FsInode) o;

        // FIXME: _jdbcFs it the part of the test
        return _level == otherInode._level && _ino == otherInode._ino && _type == otherInode._type;

        // return this.toFullString().equals( otherInode.toFullString() ) ;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(_ino);
    }

    // only package classes allowed to use this
    private boolean _ioEnabled;
    private boolean _ioFlagUpToDate;

    boolean isIoEnabled() {
        if (!_ioFlagUpToDate) {
            try {
                _ioEnabled = _fs.isIoEnabled(this);
            } catch (ChimeraFsException e) {
            }
            _ioFlagUpToDate = true;
        }
        return _ioEnabled;
    }

    public int getLevel() {
        return _level;
    }

    public DirectoryStreamB<ChimeraDirectoryEntry> newDirectoryStream() throws ChimeraFsException {
        return _fs.newDirectoryStream(this);
    }

    public DirectoryStreamB<ChimeraDirectoryEntry> virtualDirectoryStream(String labelname)
          throws ChimeraFsException {
        return _fs.virtualDirectoryStream(this, labelname);
    }

    public DirectoryStreamB<ChimeraDirectoryEntry> listLabelsStream() throws ChimeraFsException {
        return _fs.listLabelsStream(this);
    }

    public String getId() throws ChimeraFsException {
        Stat stat = _stat;
        return (stat != null) ? stat.getId() : _fs.inode2id(this);
    }
}
