// $Id: FileMetaData.java,v 1.15 2007-07-26 13:42:47 tigran Exp $
package diskCacheV111.util;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.*;

public class FileMetaData implements Serializable {

    private final static String DATE_FORMAT = "MM.dd-HH:mm:ss";

    /*
     * each field has value and isSetXXX
     */

    private int _uid = -1;
    private boolean _isUidSet;

    private int _gid = -1;
    private boolean _isGidSet;

    private long _size;
    private boolean _isSizeSet;

    private long _created;

    private long _lastAccessed;
    private boolean _isATimeSet;

    private long _lastModified;
    private boolean _isMTimeSet;

    private boolean _isRegular = true;
    private boolean _isDirectory;
    private boolean _isLink;

    private Permissions _user = new Permissions();
    private Permissions _group = new Permissions();
    private Permissions _world = new Permissions();

    private boolean _isUserPermissionSet;
    private boolean _isGroupPermissionSet;
    private boolean _isWorldPermissionSet;

    private static final long serialVersionUID = -6379734483795645452L;

    /*
     * immutable
     */
    public static class Permissions implements Serializable {

        private final boolean _canRead;
        private final boolean _canWrite;
        private final boolean _canExecute;
        private final int _perm;

        private static final long serialVersionUID = -1340210599513069884L;

        public Permissions() {
            this(0);
        }

        public Permissions(int perm) {
            _perm = perm;
            _canRead = (perm & 0x4) > 0;
            _canWrite = (perm & 0x2) > 0;
            _canExecute = (perm & 0x1) > 0;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof Permissions)) {
                return false;
            }

            return ((Permissions) obj)._perm == _perm;
        }

        @Override
        public int hashCode() {
            return _perm;
        }

        public boolean canRead() {
            return _canRead;
        }

        public boolean canWrite() {
            return _canWrite;
        }

        public boolean canExecute() {
            return _canExecute;
        }

        public boolean canLookup() {
            return canExecute();
        }

        @Override
        public String toString() {
            return (_canRead ? "r" : "-") + (_canWrite ? "w" : "-")
                    + (_canExecute ? "x" : "-");
        }
    }

    /**
     * Suitable for setFileMetaData methods, where all _isXxxSet fields id false
     * and only one field can be defined and setted
     *
     */
    public FileMetaData() {
    }

    /**
     *
     * @param uid
     * @param gid
     * @param permissions
     *            in unix format
     */
    public FileMetaData(int uid, int gid, int permissions) {
        this(false, uid, gid, permissions);
    }

    public FileMetaData(boolean isDirectory, int uid, int gid, int permission) {
        _uid = uid;
        _gid = gid;

        _isUidSet = true;
        _isGidSet = true;

        setMode(permission);
        setFileType(!isDirectory, isDirectory, false);
    }

    public FileMetaData(FileAttributes attributes)
    {
        for (FileAttribute attribute: attributes.getDefinedAttributes()) {
            switch (attribute) {
            case OWNER:
                setUid(attributes.getOwner());
                break;

            case OWNER_GROUP:
                setGid(attributes.getGroup());
                break;

            case MODE:
                setMode(attributes.getMode());
                break;

            case TYPE:
                switch (attributes.getFileType()) {
                case DIR:
                    setFileType(false, true, false);
                    break;
                case REGULAR:
                    setFileType(true, false, false);
                    break;
                case LINK:
                    setFileType(false, false, true);
                    break;
                case SPECIAL:
                    setFileType(false, false, false);
                    break;
                }
                break;

            case SIZE:
                setSize(attributes.getSize());
                break;

            case CREATION_TIME:
                _created = attributes.getCreationTime();
                break;

            case ACCESS_TIME:
                setLastAccessedTime(attributes.getAccessTime());
                break;

            case MODIFICATION_TIME:
                setLastModifiedTime(attributes.getModificationTime());
                break;
            }
        }
    }

    /**
     * set file system object type
     * @param isRegular
     * @param isDirectory
     * @param isLink
     * @throws IllegalArgumentException if more than one of the field is true
     */
    public void setFileType(boolean isRegular, boolean isDirectory,
            boolean isLink) throws IllegalArgumentException {

        if( (isRegular && isDirectory ) ) {
            throw new IllegalArgumentException("can't be file and directory at the same time");
        }

        if( (isRegular && isLink ) ) {
            throw new IllegalArgumentException("can't be file and link at the same time");
        }

        if( (isLink && isDirectory ) ) {
            throw new IllegalArgumentException("can't be link and directory at the same time");
        }

        if( (isRegular && isDirectory ) ) {
            throw new IllegalArgumentException("can't be file and directory at the same time");
        }

        if( !(isRegular || isDirectory || isLink) ) {
            throw new IllegalArgumentException("have to be a file or a directory  or a link");
        }


        _isRegular = isRegular;
        _isDirectory = isDirectory;
        _isLink = isLink;
    }

    public long getFileSize() {
        return _size;
    }

    public void setSize(long size) {
        _size = size;
        _isSizeSet = true;
    }

    public boolean isSizeSet() {
        return _isSizeSet;
    }

    public long getCreationTime() {
        return _created;
    }

    public long getLastModifiedTime() {
        return _lastModified;
    }

    public long getLastAccessedTime() {
        return _lastAccessed;
    }

    /**
     *
     * @param accessed
     *            in milliseconds
     * @param modified
     *            in milliseconds
     * @param created
     *            in milliseconds
     */
    public void setTimes(long accessed, long modified, long created) {
        _created = created;

        this.setLastAccessedTime(accessed);
        this.setLastModifiedTime(modified);
    }

    /**
     * set files last access time
     *
     * @param newTime
     *            in milliseconds
     */
    public void setLastAccessedTime(long newTime) {
        _lastAccessed = newTime;
        _isATimeSet = true;
    }

    public boolean isATimeSet() {
        return _isATimeSet;
    }

    /**
     * set files last modification time
     *
     * @param newTime
     *            in milliseconds
     */

    public void setLastModifiedTime(long newTime) {
        _lastModified = newTime;
        _isMTimeSet = true;
    }

    public boolean isMTimeSet() {
        return _isMTimeSet;
    }

    public boolean isDirectory() {
        return _isDirectory;
    }

    public boolean isSymbolicLink() {
        return _isLink;
    }

    public boolean isRegularFile() {
        return _isRegular;
    }

    public FileType getFileType()
    {
        if (isSymbolicLink()) {
            return FileType.LINK;
        }
        if (isDirectory()) {
            return FileType.DIR;
        }
        if (isRegularFile()) {
            return FileType.REGULAR;
        }
        return FileType.SPECIAL;
    }

    public void setMode(int mode)
    {
        setUserPermissions(new Permissions((mode >> 6) & 0x7));
        setGroupPermissions(new Permissions((mode >> 3) & 0x7));
        setWorldPermissions(new Permissions(mode & 0x7));
    }

    /** Returns POSIX.1 file mode. */
    public int getMode()
    {
        return _user._perm << 6 | _group._perm << 3 | _world._perm;
    }

    public Permissions getUserPermissions() {
        return _user;
    }

    private void setUserPermissions(Permissions userPermissions) {
        _user = userPermissions;
        _isUserPermissionSet = true;
    }

    public boolean isUserPermissionsSet() {
        return _isUserPermissionSet;
    }

    public Permissions getGroupPermissions() {
        return _group;
    }

    private void setGroupPermissions(Permissions groupPermissions) {
        _group = groupPermissions;
        _isGroupPermissionSet = true;
    }

    public boolean isGroupPermissionsSet() {
        return _isGroupPermissionSet;
    }

    public Permissions getWorldPermissions() {
        return _world;
    }

    private void setWorldPermissions(Permissions worldPermissions) {
        _world = worldPermissions;
        _isWorldPermissionSet = true;
    }

    public boolean isWorldPermissionsSet() {
        return _isWorldPermissionSet;
    }

    public int getUid() {
        return _uid;
    }

    public void setUid(int newUid) {
        _uid = newUid;
        _isUidSet = true;
    }

    public boolean isUidSet() {
        return _isUidSet;
    }

    public int getGid() {
        return _gid;
    }

    public void setGid(int newGid) {
        _gid = newGid;
        _isGidSet = true;
    }

    public boolean isGidSet() {
        return _isGidSet;
    }

    public String getPermissionString() {
        return (_isDirectory ? "d" : _isLink ? "l" : _isRegular ? "-" : "x")
                + _user + _group + _world;
    }

    /**
     * Compares the permissions flags of this FileMetaData to another
     * FileMetaData. They are considered equal if both group, user and
     * world permissions are equal.
     *
     * @return true if the argument is not null and the its
     *         permissions are equals to this FileMetaData; false
     *         otherwise
     */
    public boolean equalsPermissions(FileMetaData other)
    {
        return other != null
            && getGroupPermissions().equals(other.getGroupPermissions())
            && getUserPermissions().equals(other.getUserPermissions())
            && getWorldPermissions().equals(other.getWorldPermissions());
    }

    @Override
    public String toString() {
        SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
        return "["
                + (_isDirectory ? "d" : _isLink ? "l" : _isRegular ? "-" : "x")
                + _user + _group + _world + ";" + _uid + ";" + _gid + "]"
                + "[c=" + formatter.format(new Date(_created)) + ";m="
                + formatter.format(new Date(_lastModified)) + ";a="
                + formatter.format(new Date(_lastAccessed)) + "]";
    }

    @Override
    public boolean equals(Object obj) {

        if(obj == this) {
            return true;
        }

        if( !(obj instanceof FileMetaData ) ) {
            return false;
        }

        FileMetaData other = (FileMetaData)obj;

        return
            // owner and group
               other._gid == this._gid
            && other._uid == this._uid

            // type
            && other._isDirectory == this._isDirectory
            && other._isLink == this._isLink
            && other._isRegular == this._isRegular

            /**
             *  size.
             *
             *  NB if both are directories we ignore the size.  This
             *  is because both PNFS and Chimera return dummy values.
             */
            && (other._size == this._size || this._isDirectory)

            // permissions
            && other._user.equals(this._user)
            && other._group.equals(this._group)
            && other._world.equals(this._world)

            // times
            && other._created == this._created
            && other._lastAccessed == this._lastAccessed
            && other._lastModified == this._lastModified

            ;

    }

    @Override
    public int hashCode() {
        return 17; // to force collections to check with equals()
    }

    public FileAttributes toFileAttributes()
    {
        FileAttributes attributes = new FileAttributes();

        if (_isUidSet) {
            attributes.setOwner(_uid);
        }

        if (_isGidSet) {
            attributes.setGroup(_gid);
        }

        if (_isSizeSet) {
            attributes.setSize(_size);
        }

        if (_created > 0) {
            attributes.setCreationTime(_created);
        }

        if (_isATimeSet) {
            attributes.setAccessTime(_lastAccessed);
        }

        if (_isMTimeSet) {
            attributes.setModificationTime(_lastModified);
        }

        if (_isUserPermissionSet || _isGroupPermissionSet || _isWorldPermissionSet) {
            attributes.setMode(getMode());
        }

        attributes.setFileType(getFileType());

        return attributes;
    }

    /**
     * Returns the set of FileAttributes that FileMetaData can
     * represent.
     */
    public static Set<FileAttribute> getKnownFileAttributes()
    {
        return EnumSet.of(OWNER, OWNER_GROUP, MODE, TYPE, SIZE,
                          CREATION_TIME, ACCESS_TIME, MODIFICATION_TIME);
    }
}
