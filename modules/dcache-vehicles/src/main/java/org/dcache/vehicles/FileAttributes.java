package org.dcache.vehicles;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.acl.ACL;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.Checksum;

import static java.util.stream.Collectors.toMap;
import static org.dcache.namespace.FileAttribute.*;

/**
 * <code>FileAttributes</code> encapsulates attributes about a logical file.
 *
 * The attributes represented by an instance of this class belong to
 * a logical file as seen by a client or user of dCache. That is,
 * FileAttributes represent the information about a file stored, or that
 * should be stored, in the name space or other central components.
 *
 * Besides their location, the class does not represent any properties
 * of physical replicas on a pool. Eg the size or checksum stored in a
 * FileAttributes instance represents the expected file size and expected
 * checksum of the file. An broken replica may have a different size or a
 * different checksum.
 *
 * The distinction between the logical and physical instance is relevant when
 * considering response types to pool query messages: These should NOT return
 * attributes of a replica using FileAttributes, except when those represent
 * cached information from the name space or other central components.
 *
 * Not all attributes may be defined. Attempts to read undefined attributes
 * will throw IllegalStateException.
 *
 * @since 1.9.5
 */
public class FileAttributes implements Serializable {


    private static final long serialVersionUID = -3689129805631724432L;

    /**
     * Set of attributes which have been set.
     */
    private Set<FileAttribute> _definedAttributes =
        EnumSet.noneOf(FileAttribute.class);

    /**
     * NFSv4 Access control list.
     */
    private ACL _acl;

    /**
     * file's size
     */
    private long _size;

    /**
     * file's attribute change time
     */
    private long _ctime;

    /**
     * file's creation time
     */
    private long _creationTime;

    /**
     * file's last access time
     */
    private long _atime;

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
     * POSIX.1 file mode
     */
    private int _mode;

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
     * File locations within dCache.
     */
    private Collection<String> _locations;

    /**
     * Key value map of flags associated with the file.
     */
    private Map<String, String> _flags;

    /**
     * The unique PNFS ID of a file.
     */
    private PnfsId _pnfsId;

    /**
     * The storage info of a file.
     */
    private StorageInfo _storageInfo;

    /**
     * The storage class of a file.
     */
    private String _storageClass;

    /**
     * The HSM of a file.
     */
    private String _hsm;

    /**
     * The cache class of a file.
     */
    private String _cacheClass;

    /** Throws IllegalStateException if attribute is not defined. */
    private void guard(FileAttribute attribute)
        throws IllegalStateException
    {
        if (!_definedAttributes.contains(attribute)) {
            throw new IllegalStateException("Attribute is not defined: " +
                    attribute);
        }
    }

    private void define(FileAttribute attribute)
    {
        _definedAttributes.add(attribute);
    }

    public void undefine(FileAttribute attribute)
    {
        _definedAttributes.remove(attribute);
    }

    public boolean isUndefined(FileAttribute attribute)
    {
        return !_definedAttributes.contains(attribute);
    }

    public boolean isDefined(FileAttribute attribute)
    {
        return _definedAttributes.contains(attribute);
    }

    public boolean isDefined(Set<FileAttribute> attributes)
    {
        return _definedAttributes.containsAll(attributes);
    }

    /**
     * Get the set of available attributes. The set may have zero or
     * more entries.
     * @return set of defined attribute.
     */
    @Nonnull
    public Set<FileAttribute> getDefinedAttributes() {
        return _definedAttributes;
    }

    @Nonnull
    public AccessLatency getAccessLatency() {
        guard(ACCESS_LATENCY);
        return _accessLatency;
    }

    @Nonnull
    public Optional<AccessLatency> getAccessLatencyIfPresent() {
        return toOptional(ACCESS_LATENCY, _accessLatency);
    }

    public long getAccessTime()
    {
        guard(ACCESS_TIME);
        return _atime;
    }

    @Nonnull
    public ACL getAcl()
    {
        guard(ACL);
        return _acl;
    }

    @Nonnull
    public Set<Checksum> getChecksums() {
        guard(CHECKSUM);
        return _checksums;
    }

    @Nonnull
    public Optional<Set<Checksum>> getChecksumsIfPresent() {
        return toOptional(CHECKSUM, _checksums);
    }

    /**
     * Get {@link FileType} corresponding to the file.
     * @return file type
     */
    @Nonnull
    public FileType getFileType() {
        guard(TYPE);
        return _fileType;
    }

    /**
     * Get group id to which file belongs to.
     * @return group id
     */
    public int getGroup() {
        guard(OWNER_GROUP);
        return _group;
    }

    public int getMode() {
        guard(MODE);
        return _mode;
    }

    /**
     * Get file's attribute change time.
     *
     * @return time in milliseconds since 1 of January 1970 00:00.00
     */
    public long getChangeTime() {
        guard(CHANGE_TIME);
        return _ctime;
    }

    /**
     * Get file's creation time.
     * @return time in milliseconds since 1 of January 1970 00:00.00
     */
    public long getCreationTime() {
        guard(CREATION_TIME);
        return _creationTime;
    }

    /**
     * Get file's last modification time.
     * @return time in milliseconds since 1 of January 1970 00:00.00
     */
    public long getModificationTime() {
        guard(MODIFICATION_TIME);
        return _mtime;
    }

    /**
     * Get owner id to whom file belongs to.
     * @return owner id
     */
    public int getOwner() {
        guard(OWNER);
        return _owner;
    }

    @Nonnull
    public RetentionPolicy getRetentionPolicy() {
        guard(RETENTION_POLICY);
        return _retentionPolicy;
    }

    @Nonnull
    public Optional<RetentionPolicy> getRetentionPolicyIfPresent() {
        return toOptional(RETENTION_POLICY, _retentionPolicy);
    }

    public long getSize() {
        guard(SIZE);
        return _size;
    }

    public Optional<Long> getSizeIfPresent() {
        return toOptional(SIZE, _size);
    }

    @Nonnull
    public PnfsId getPnfsId()
    {
        guard(PNFSID);
        return _pnfsId;
    }

    @Nonnull
    public StorageInfo getStorageInfo()
    {
        guard(STORAGEINFO);
        return _storageInfo;
    }

    public void setAccessTime(long atime)
    {
        define(ACCESS_TIME);
        _atime = atime;
    }

    public void setAccessLatency(AccessLatency accessLatency) {
        define(ACCESS_LATENCY);
        _accessLatency = accessLatency;
    }

    public void setAcl(ACL acl)
    {
        define(ACL);
        _acl = acl;
    }

    public void setChecksums(Set<Checksum> checksums) {
        define(CHECKSUM);
        _checksums = checksums;
    }

    public void setFileType(FileType fileType) {
        define(TYPE);
        _fileType = fileType;
    }

    public void setGroup(int group) {
        define(OWNER_GROUP);
        _group = group;
    }

    public void setMode(int mode) {
        define(MODE);
        _mode = mode;
    }

    public void setChangeTime(long ctime) {
        define(CHANGE_TIME);
        _ctime = ctime;
    }

    public void setCreationTime(long creationTime) {
        define(CREATION_TIME);
        _creationTime = creationTime;
    }

    public void setModificationTime(long mtime) {
        define(MODIFICATION_TIME);
        _mtime = mtime;
    }

    public void setOwner(int owner) {
        define(OWNER);
        _owner = owner;
    }

    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        define(RETENTION_POLICY);
        _retentionPolicy = retentionPolicy;
    }

    public void setSize(long size) {
        define(SIZE);
        _size = size;
    }

    public void setLocations(Collection<String> pools) {
        define(LOCATIONS);
        _locations = pools;
    }

    @Nonnull
    public Collection<String> getLocations() {
        guard(LOCATIONS);
        return _locations;
    }

    @Nonnull
    public Map<String, String> getFlags() {
        guard(FLAGS);
        return _flags;
    }

    public void setFlags(Map<String, String> flags) {
        define(FLAGS);
        _flags = flags;
    }

    public void setPnfsId(PnfsId pnfsId)
    {
        define(PNFSID);
        _pnfsId = pnfsId;
    }

    public void setStorageInfo(StorageInfo storageInfo)
    {
        define(STORAGEINFO);
        _storageInfo = storageInfo;
    }

    public String getStorageClass()
    {
        guard(STORAGECLASS);
        return _storageClass;
    }

    public void setStorageClass(String storageClass)
    {
        define(STORAGECLASS);
        _storageClass = storageClass;
    }

    public void setCacheClass(String cacheClass)
    {
        define(CACHECLASS);
        _cacheClass = Strings.nullToEmpty(cacheClass); // For compatibility with pre-2.12 - remove after next golden
    }

    public String getCacheClass()
    {
        guard(CACHECLASS);
        return Strings.emptyToNull(_cacheClass);   // For compatibility with pre-2.12 - remove after next golden
    }

    public void setHsm(String hsm)
    {
        define(HSM);
        _hsm = hsm;
    }

    public String getHsm()
    {
        guard(HSM);
        return _hsm;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("defined", _definedAttributes)
                .add("acl", _acl)
                .add("size", _size)
                .add("ctime", _ctime)
                .add("creationTime", _creationTime)
                .add("atime", _atime)
                .add("mtime", _mtime)
                .add("checksums", _checksums)
                .add("owner", _owner)
                .add("group", _group)
                .add("mode", _mode)
                .add("accessLatency", _accessLatency)
                .add("retentionPolicy", _retentionPolicy)
                .add("fileType", _fileType)
                .add("locations", _locations)
                .add("flags", _flags)
                .add("pnfsId", _pnfsId)
                .add("storageInfo", _storageInfo)
                .add("storageClass", _storageClass)
                .add("cacheClass", _cacheClass)
                .add("hsm", _hsm)
                .omitNullValues()
                .toString();
    }

    @Nonnull
    private <T> Optional<T> toOptional(FileAttribute attribute, T value)
    {
        return isDefined(attribute) ? Optional.of(value) :
                Optional.<T>absent();
    }

    private void writeObject(ObjectOutputStream stream)
            throws IOException
    {
        boolean wasCacheClassDefined = _definedAttributes.remove(CACHECLASS); // For compatibility with pre-2.12 - remove after next golden
        stream.defaultWriteObject();
        if (wasCacheClassDefined) {
            _definedAttributes.add(CACHECLASS);
        }
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_flags != null) {
            _flags = _flags.entrySet().stream().collect(toMap(e -> e.getKey().intern(), e -> e.getValue()));
        }
        if (_storageClass != null) {
            _storageClass = _storageClass.intern();
        }
        if (_cacheClass != null) {
            _cacheClass = _cacheClass.intern();
            _definedAttributes.add(CACHECLASS); // For compatibility with pre-2.12 - remove after next golden
        }
        if (_hsm != null) {
            _hsm = _hsm.intern();
        }
    }
}
