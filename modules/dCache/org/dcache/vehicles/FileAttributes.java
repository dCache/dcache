package org.dcache.vehicles;

import org.dcache.namespace.FileType;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Checksum;

/**
 *
 * <code>FileAttributes</code> is a set off all  file attributes
 * used by dcache including <code>StorageInfo</code> and <code>Location</code>.
 * The only part of attributes may be defined. If client asks for an attribute
 * which is not in returned by <code>attributes()</code> then return value is
 * unpredictable.
 *
 * @since 1.9.5
 */
public class FileAttributes implements Serializable {


    private static final long serialVersionUID = -3689129805631724432L;

    /**
     * array of provided attribute types
     */
    private FileAttribute[] _definedAttributes;

    /**
     * file's size
     */
    private long _size;

    /**
     * file's change time. This is a time then files metadata was modified.
     */
    private long _ctime;

    /**
     * file's last modification time
     */
    private long _mtime;

    /**
     * file's known checksums
     */
    private Set<Checksum> _checksums;

    /**
     * file's owner's id
     */
    private int _owner;

    /**
     * file's group id
     */
    private int _group;

    /**
     * file's access latency ( e.g. ONLINE/NEARLINE )
     */
    private AccessLatency _accessLatency;

    /**
     * file's retention policy ( e.g. CUSTODIAL/REPLICA )
     */
    private RetentionPolicy _retentionPolicy;

    /**
     * type of the file ( e.g. REG, DIR, LINK, SPECIAL )
     */
    private FileType _fileType;

    /**
     * File location within dCache.
     */
    private String _location;

    /**
     * Key value map of flags acossiated with the file.
     */
    private Map<String, String> _flags;

    /**
     * Get list of available attribute. The array may have zero or more entries.
     * @return array of defined attribute.
     */
    public FileAttribute[] getDefinedAttributes() {
        return _definedAttributes;
    }

    /**
     * Get list of available attribute. The array may have zero or more entries.
     * @return array of defined attribute.
     */
    public void setDefinedAttributes(FileAttribute ... attr) {
         this._definedAttributes = attr;
    }

    public AccessLatency getAccessLatency() {
        return _accessLatency;
    }

    public Set<Checksum> getChecksums() {
        return _checksums;
    }

    public long getChangeTime() {
        return _ctime;
    }

    /**
     * Get {@link FileType} corresponding to the file.
     * @return file type
     */
    public FileType getFileType() {
        return _fileType;
    }

    /**
     * Get group id to which file belongs to.
     * @return group id
     */
    public int getGroup() {
        return _group;
    }

    /**
     * Get file's last modification time.
     * @return time in milliseconds since 1 of January 1970 00:00.00
     */
    public long getModificationTime() {
        return _mtime;
    }

    /**
     * Get owner id to whom file belongs to.
     * @return owner id
     */
    public int getOwner() {
        return _owner;
    }

    public RetentionPolicy getRetentionPolicy() {
        return _retentionPolicy;
    }

    public long getSize() {
        return _size;
    }

    public void setAccessLatency(AccessLatency accessLatency) {
        _accessLatency = accessLatency;
    }

    public void setChecksums(Set<Checksum> checksums) {
        _checksums = checksums;
    }

    public void setChangeTime(long ctime) {
        _ctime = ctime;
    }

    public void setFileType(FileType fileType) {
        _fileType = fileType;
    }

    public void setGroup(int group) {
        _group = group;
    }

    public void setModificationTime(long mtime) {
        _mtime = mtime;
    }

    public void setOwner(int owner) {
        _owner = owner;
    }

    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        _retentionPolicy = retentionPolicy;
    }

    public void setSize(long size) {
        _size = size;
    }

    public void setLocation(String pool) {
        _location = pool;
    }

    public String getLocation() {
        return _location;
    }

    public Map<String, String> getFlags() {
        return _flags;
    }

    public void setFlags(Map<String, String> flags) {
        _flags = flags;
    }
}
