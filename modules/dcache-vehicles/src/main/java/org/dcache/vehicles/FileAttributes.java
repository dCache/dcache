package org.dcache.vehicles;

import com.google.common.base.MoreObjects;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.acl.ACL;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.Checksum;

import static java.util.Arrays.asList;
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
public class FileAttributes implements Serializable, Cloneable {


    private static final long serialVersionUID = -3689129805631724432L;

    /**
     * Set of attributes which have been set.
     */
    private final EnumSet<FileAttribute> _definedAttributes =
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
     * @since 3.0
     */
    private int _nlink;

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

    @Override
    public FileAttributes clone()
    {
        try {
            FileAttributes clone = (FileAttributes) super.clone();

            if (isDefined(ACL)) {
                clone.setAcl(getAcl());
            }

            if (isDefined(SIZE)) {
                clone.setSize(getSize());
            }

            if (isDefined(CHANGE_TIME)) {
                clone.setChangeTime(getChangeTime());
            }

            if (isDefined(CREATION_TIME)) {
                clone.setCreationTime(getCreationTime());
            }

            if (isDefined(ACCESS_TIME)) {
                clone.setAccessTime(getAccessTime());
            }

            if (isDefined(MODIFICATION_TIME)) {
                clone.setModificationTime(getModificationTime());
            }

            if (isDefined(CHECKSUM)) {
                Set<Checksum> checksums = new HashSet<>(getChecksums());
                clone.setChecksums(checksums);
            }

            if (isDefined(OWNER)) {
                clone.setOwner(getOwner());
            }

            if (isDefined(OWNER_GROUP)) {
                clone.setGroup(getGroup());
            }

            if (isDefined(MODE)) {
                clone.setMode(getMode());
            }

            if (isDefined(NLINK)) {
                clone.setNlink(getNlink());
            }

            if (isDefined(ACCESS_LATENCY)) {
                clone.setAccessLatency(getAccessLatency());
            }

            if (isDefined(RETENTION_POLICY)) {
                clone.setRetentionPolicy(getRetentionPolicy());
            }

            if (isDefined(TYPE)) {
                clone.setFileType(getFileType());
            }

            if (isDefined(LOCATIONS)) {
                Collection<String> locations = new ArrayList<>(getLocations());
                clone.setLocations(locations);
            }

            if (isDefined(FLAGS)) {
                Map<String,String> flags = new HashMap<>(getFlags());
                clone.setFlags(flags);
            }

            if (isDefined(PNFSID)) {
                clone.setPnfsId(getPnfsId());
            }

            if (isDefined(STORAGEINFO)) {
                StorageInfo oldValue = getStorageInfo();
                clone.setStorageInfo(oldValue.clone());
            }

            if (isDefined(STORAGECLASS)) {
                clone.setStorageClass(getStorageClass());
            }

            if (isDefined(HSM)) {
                clone.setHsm(getHsm());
            }

            if (isDefined(CACHECLASS)) {
                clone.setCacheClass(getCacheClass());
            }

            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Bad clone: " + e, e);
        }
    }

    /** Throws IllegalStateException if attribute is not defined. */
    private void guard(FileAttribute attribute)
        throws IllegalStateException
    {
        if (!_definedAttributes.contains(attribute)) {
            throw new IllegalStateException("Attribute is not defined: " +
                    attribute);
        }
    }

    private void define(FileAttribute... attributes)
    {
        _definedAttributes.addAll(asList(attributes));
    }

    public void undefine(FileAttribute... attributes)
    {
        _definedAttributes.removeAll(asList(attributes));
    }

    public boolean isUndefined(FileAttribute attribute)
    {
        return !_definedAttributes.contains(attribute);
    }

    /**
     * @return true iff all attributes are not define.
     */
    public boolean isUndefined(Set<FileAttribute> attributes)
    {
        return EnumSet.complementOf(_definedAttributes).containsAll(attributes);
    }

    public boolean isDefined(FileAttribute attribute)
    {
        return _definedAttributes.contains(attribute);
    }

    /**
     * @return true iff all attributes are defined.
     */
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

    public Optional<Integer> getGroupIfPresent() {
        return toOptional(OWNER_GROUP, _group);
    }

    public int getMode() {
        guard(MODE);
        return _mode;
    }

    public Optional<Integer> getModeIfPresent() {
        return toOptional(MODE, _mode);
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

    public Optional<Integer> getOwnerIfPresent() {
        return toOptional(OWNER, _owner);
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

    public void setPnfsId(String pnfsId)
    {
        setPnfsId(new PnfsId(pnfsId));
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
        _cacheClass = cacheClass;
    }

    public String getCacheClass()
    {
        guard(CACHECLASS);
        return _cacheClass;
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

    public void setNlink(int nlink)
    {
        define(NLINK);
        _nlink = nlink;
    }

    public int getNlink()
    {
        guard(NLINK);
        return _nlink;
    }

    /**
     * Remove the {@link FileType} corresponding to the file.  The FileType
     * must be specified before this method is called.  Subsequent getFileType
     * or removeFileType will fail unless setFileType is called subsequently.
     *
     * @return the removed file type
     * @throws IllegalStateException if the FileType is not specified.
     */
    @Nonnull
    public FileType removeFileType() {
        guard(TYPE);
        undefine(TYPE);
        return _fileType;
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
        return isDefined(attribute) ? Optional.of(value) : Optional.empty();
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
            if (_cacheClass.isEmpty()) {
                _cacheClass = null;  // For compatibility with pre-2.17- remove after next golden
            } else {
                _cacheClass = _cacheClass.intern();
            }
        }
        if (_hsm != null) {
            _hsm = _hsm.intern();
        }
    }

    public static FileAttributes ofAccessTime(long when)
    {
        return of().accessTime(when).build();
    }

    public static FileAttributes ofAcl(ACL acl)
    {
        return of().acl(acl).build();
    }

    public static FileAttributes ofChecksum(Checksum value)
    {
        return of().checksum(value).build();
    }

    public static FileAttributes ofCreationTime(long when)
    {
        return of().creationTime(when).build();
    }

    public static FileAttributes ofFlag(String name, String value)
    {
        return of().flag(name, value).build();
    }

    public static FileAttributes ofFlags(Map<String,String> flags)
    {
        return of().flags(flags).build();
    }

    public static FileAttributes ofGid(int gid)
    {
        return of().gid(gid).build();
    }

    public static FileAttributes ofMode(int mode)
    {
        return of().mode(mode).build();
    }

    public static FileAttributes ofModificationTime(long when)
    {
        return of().modificationTime(when).build();
    }

    public static FileAttributes ofPnfsId(String id)
    {
        return of().pnfsId(id).build();
    }

    public static FileAttributes ofPnfsId(PnfsId id)
    {
        return of().pnfsId(id).build();
    }

    public static FileAttributes ofSize(long size)
    {
        return of().size(size).build();
    }

    public static FileAttributes ofStorageInfo(StorageInfo info)
    {
        return of().storageInfo(info).build();
    }

    public static FileAttributes ofFileType(FileType type)
    {
        return of().fileType(type).build();
    }

    public static FileAttributes ofLocation(String pool)
    {
        return of().location(pool).build();
    }

    public static FileAttributes ofLocations(Collection<String> pools)
    {
        return of().locations(pools).build();
    }

    public static FileAttributes ofHsm(String hsm)
    {
        return of().hsm(hsm).build();
    }

    public static FileAttributes ofStorageClass(String storageClass)
    {
        return of().storageClass(storageClass).build();
    }

    public static Builder of()
    {
        return new FileAttributes().new Builder();
    }

    public class Builder
    {
        public Builder accessLatency(AccessLatency al)
        {
            setAccessLatency(al);
            return this;
        }

        public Builder accessTime(long when)
        {
            setAccessTime(when);
            return this;
        }

        public Builder acl(ACL acl)
        {
            setAcl(acl);
            return this;
        }

        public FileAttributes build()
        {
            return FileAttributes.this;
        }

        public Builder checksum(Checksum checksum)
        {
            setChecksums(Collections.singleton(checksum));
            return this;
        }

        public Builder checksums(Set<Checksum> checksums)
        {
            setChecksums(checksums);
            return this;
        }

        public Builder creationTime(long when)
        {
            setCreationTime(when);
            return this;
        }

        public Builder fileType(FileType type)
        {
            setFileType(type);
            return this;
        }

        public Builder flag(String name, String value)
        {
            if (!isDefined(FLAGS)) {
                setFlags(new HashMap());
            }
            getFlags().put(name, value);
            return this;
        }

        public Builder flags(Map<String, String> flags)
        {
            if (!isDefined(FLAGS)) {
                setFlags(new HashMap());
            }
            getFlags().putAll(flags);
            return this;
        }

        /**
         * Add the primary gid taken from the primary GidPrincipal within the
         * Subject.
         */
        public Builder gid(Subject subject)
        {
            return gid((int)Subjects.getPrimaryGid(subject));
        }

        public Builder gid(int gid)
        {
            setGroup(gid);
            return this;
        }

        public Builder location(String pool)
        {
            if (!isDefined(LOCATIONS)) {
                setLocations(new ArrayList());
            }
            getLocations().add(pool);
            return this;
        }

        public Builder locations(Collection<String> pools)
        {
            if (!isDefined(LOCATIONS)) {
                setLocations(new ArrayList());
            }
            getLocations().addAll(pools);
            return this;
        }

        public Builder mode(int mode)
        {
            setMode(mode);
            return this;
        }

        public Builder modificationTime(long when)
        {
            setModificationTime(when);
            return this;
        }

        public Builder pnfsId(String id)
        {
            setPnfsId(id);
            return this;
        }

        public Builder pnfsId(PnfsId id)
        {
            setPnfsId(id);
            return this;
        }

        public Builder retentionPolicy(RetentionPolicy rp)
        {
            setRetentionPolicy(rp);
            return this;
        }

        public Builder size(long size)
        {
            setSize(size);
            return this;
        }

        public Builder storageInfo(StorageInfo info)
        {
            setStorageInfo(info);
            return this;
        }

        /**
         *  Add the uid taken from the UidPrincipal within the Subject.
         */
        public Builder uid(Subject subject)
        {
            return uid((int)Subjects.getUid(subject));
        }

        public Builder uid(int uid)
        {
            setOwner(uid);
            return this;
        }

        public Builder hsm(String hsm)
        {
            setHsm(hsm);
            return this;
        }

        public Builder storageClass(String storageClass)
        {
            setStorageClass(storageClass);
            return this;
        }

        public Builder cacheClass(String cacheClass)
        {
            setCacheClass(cacheClass);
            return this;
        }
    }
}
