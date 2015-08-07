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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.dcache.acl.ACE;
import org.dcache.chimera.posix.Stat;

/**
 * inode representation
 */
public class FsInode {

    private static final int NIBBLES_IN_32_BIT_INTEGER = 8;
    private static final int RADIX_HEXADECIMAL = 16;
    private static final int ID_LENGTH_PNFS_ID = 24;
    // we are not allowed to modify it
    protected final String _id;
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
     * parent inode. In case of hard links, one of the
     * possible parents.
     */
    private FsInode _parent;

    /**
     * create a new inode in filesystem fs with given id and type
     * @param fs file system
     * @param id the new id
     * @param type inode type
     */
    public FsInode(FileSystemProvider fs, String id, FsInodeType type) {
        this(fs, id, type, 0);
    }

    /**
     * create a new inode of type 'inode' in the filesystem fs with given id
     * @param fs file system
     * @param id inode id
     */
    public FsInode(FileSystemProvider fs, String id) {
        this(fs, id, 0);
    }

    /**
     * Create a new FsInode with given id and level,  type == INODE
     * @param fs
     * @param id
     * @param level
     */
    public FsInode(FileSystemProvider fs, String id, int level) {
        this(fs, id, FsInodeType.INODE, level);
    }

    /**
     * Create a new FsInode with given id, type and level
     *
     * @param fs
     * @param id
     * @param type
     * @param level
     */
    public FsInode(FileSystemProvider fs, String id, FsInodeType type, int level) {
        _id = id;
        _fs = fs;
        _level = level;
        _type = type;
    }

    /**
     * Create a new FsInode with newly generated id, type == INODE, level==0
     * @param fs
     */
    public FsInode(FileSystemProvider fs) {
        this(fs, FsInode.generateNewID());
    }

    public long id() {
        long id;

        if (_id.length() == ID_LENGTH_PNFS_ID) {
            id = buildPnfsInodeId();
        } else {
            id = buildDecodedAndXoredInodeId();
        }

        return id;
    }

    private long buildPnfsInodeId() {

        /*
         * PNFS-IDs are 24-character hexadecimal strings of the form:
         *
         *      ddddeeeehhhhhhhhllllllll
         *
         *  where d are nibbles of 16-bit database
         *        e are nibbles of 16-bit ext(ended) counter (which is ignored)
         *        h are nibbles of 32-bit high-order counter (mostly ignored)
         *        l are nibbles of 32-bit low-order counter
         */
        int database = Integer.parseInt(_id.substring(0, 4), RADIX_HEXADECIMAL);
        long high = Long.parseLong(_id.substring(8, 16), RADIX_HEXADECIMAL);
        long low = Long.parseLong(_id.substring(16), RADIX_HEXADECIMAL);

        /*
         *  The three lowest-order bits in a PNFS-ID represent the different levels
         *  of a file.  Because of this, new PNFS-IDs are created by increasing the
         *  largest PNFS-ID by 8.  Since the lowest 3-bits are not significant, we
         *  shift the least-significant 32-bit part of the PNFS-ID ("low") down by
         *  3-bits to allow 3-bits from the next least-significant 32-bit part of
         *  the PNFS-ID ("high").
         */
        long counter = low >> 3 | (high & 7) << 29;

        /*
         *  reverse order (LSB becomes MSB, etc) and left-shift by 16-bits.
         *  For example:
         *         (0000000000000000)0000000000000001
         *  becomes 1000000000000000 0000000000000000
         *
         *  and    (0000000000000000)1111111111101111
         *  becomes 1111011111111111 0000000000000000
         */
        int reversedDatabase = Integer.reverse(database);

        /*
         * We reverse the bits of the database to reduce the impact; for example,
         * if a site has 7 databases then only 3-bits are needed to represent the
         * database.  The reversedDatabase would take up only the 3 most-significant
         * bits and remaining 29-bits can represent 2^30-1 (approx 10^9) entries
         * before there is any chance of collisions.
         *
         * PNFS maps PNFS-ID to inode-ID in a similar fashion, but with explicit
         * partitioning and taking only the least-significant 8-bits of the database.
         * From dbfs/md2ptypes.h, line 201
         *
         *      #define  mdInodeID(id)        (((id).db<<24)|((id).low&0xFFFFF8))
         *
         * In comparison to the PNFS method, the algorithm here allows more significant
         * bits from the counter without collisions if a site has 127 or fewer databases.
         */
        long id = reversedDatabase ^ counter;

        return id & 0xFFFFFFFFL;
    }

    private long buildDecodedAndXoredInodeId() {
        long inodeId = 0;

        for (int index = 0; index < _id.length(); index += NIBBLES_IN_32_BIT_INTEGER) {
            int endIndex = index + NIBBLES_IN_32_BIT_INTEGER;
            if (endIndex > _id.length()) {
                endIndex = _id.length();
            }

            String idFragment = _id.substring(index, endIndex);
            long uint = Long.parseLong(idFragment, RADIX_HEXADECIMAL);

            inodeId ^= uint;
        }

        return inodeId;
    }

    /**
     * Generates new inode id assigned to filesystem with fsId == 0
     *
     * @return new id
     */
    public final static String generateNewID() {
        return InodeId.newID(0);
    }

    /**
     * Generates new inode id assigned to filesystem referenced by <i>fsId</>
     *
     * @param fsId
     * @return new id
     */
    public final static String generateNewID(int fsId) {
        return InodeId.newID(fsId);
    }

    /**
     *
     * @return inode's type, e.q.: inode, tag, id
     */
    public FsInodeType type() {
        return _type;
    }

    /**
     * A helper method to generate the base path of identifier.
     * @param opaque inode specific data
     * @return
     */
    protected final byte[] byteBase(byte[] opaque) {
        ByteBuffer b = ByteBuffer.allocate(128);
        byte[] fh = InodeId.hexStringToByteArray(_id);
        b.put((byte) _fs.getFsId())
                .put((byte) _type.getType())
                .put((byte) fh.length)
                .put(fh);

        b.put((byte) opaque.length);
        b.put(opaque);

        return Arrays.copyOf(b.array(), b.position());
    }

    /**
     * @return a byte[] representation of inode, including type and fsid
     */
    public byte[] getIdentifier() {
        return byteBase( Integer.toString(_level).getBytes());
    }

    @Deprecated
    /**
     * @return a String representation of inode, including type and fsid
     */
    public String toFullString() {
        return JdbcFs.toHexString(getIdentifier());
    }

    /**
     * @return a String representation of inode id only
     */
    @Override
    public String toString() {
        return _id;
    }

    /**
     *
     * gets the actual stat information of the inode and updated the cached value
     * See also statCache()
     * @return Stat
     * @throws FileNotFoundHimeraFsException
     */
    public Stat stat() throws ChimeraFsException {
        _stat = _fs.stat(this, _level);
        return _stat;
    }

    /**
     *
     * gets the cached value of  stat information of the inode
     * See also stat()
     * @return Stat
     * @throws FileNotFoundHimeraFsException
     */
    public Stat statCache() throws ChimeraFsException {
        return (_stat == null) ? stat() : _stat;
    }

    public int write(long pos, byte[] data, int offset, int len) {
        int ret = -1;
        try {
            ret = _fs.write(this, _level, pos, data, offset, len);
            _stat = null;
        } catch (ChimeraFsException e) {
        }
        return ret;
    }

    public int read(long pos, byte[] data, int offset, int len) {
        int ret = -1;
        try {
            ret = _fs.read(this, _level, pos, data, offset, len);
        } catch (ChimeraFsException e) {
        }
        return ret;
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
    public FsInode mkdir(String name, int owner, int group, int mode, List<ACE> acl, Map<String, byte[]> tags)
            throws ChimeraFsException {
        FsInode dir = _fs.mkdir(this, name, owner, group, mode, acl, tags);
        _stat = null;
        return dir;
    }

    /**
     * get inode of file in the current directory with name 'name'
     */
    public FsInode inodeOf(String name) throws ChimeraFsException {
        return _fs.inodeOf(this, name);
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
     * create new link to a specified file in the current directory with name 'name'
     * ( FsInode parent, String name , int uid, int gid, int mode, byte[] dest)
     */
    public FsInode createLink(String name, int uid, int gid, int mode, byte[] dest) throws ChimeraFsException {
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
    static public FsInode getRoot(FileSystemProvider fs) throws ChimeraFsException {
        return fs.path2inode("/");
    }

    public boolean exists() throws ChimeraFsException {

        boolean rc = false;

        try {
            statCache();
            rc = true;
        } catch (FileNotFoundHimeraFsException hfe) {
        }
        return rc;
    }

    public boolean isDirectory() {

        boolean rc = false;

        try {
            if (exists() && ((_stat.getMode() & UnixPermission.F_TYPE) == UnixPermission.S_IFDIR)) {
                rc = true;
            }
        } catch(ChimeraFsException ignore) {
        }

        return rc;
    }

    public boolean isLink() {

        boolean rc = false;

        try {
            if (exists() && new UnixPermission(_stat.getMode()).isSymLink()) {
                rc = true;
            }
        } catch(ChimeraFsException ignore) {
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
            throw new IOHimeraFsException("Not a directory");
        }

        _fs.remove(this, name, inodeOf(name));
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
                _parent = _fs.getParentOf(this);
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

    void setStatCache(Stat predefinedStat) {
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
        return _level == otherInode._level && _id.equals(otherInode._id) && _type.equals(otherInode._type);

        // return this.toFullString().equals( otherInode.toFullString() ) ;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getIdentifier());
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

    public DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream() throws ChimeraFsException {
        return _fs.newDirectoryStream(this);
    }
}
