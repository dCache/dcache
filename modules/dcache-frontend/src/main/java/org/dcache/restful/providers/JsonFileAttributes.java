package org.dcache.restful.providers;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;
import io.swagger.v3.oas.annotations.media.Schema;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.acl.ACL;
import org.dcache.namespace.FileType;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

/**
 * This class is needed to mapping FileAttributes, when they are not defined. The issue is that in
 * the system not all attributes may be defined. Attempts to read undefined attributes will throw
 * IllegalStateException( e.g. guard(CHECKSUM)). So just returning FileAttributes in
 * FileResources.getFileAttributes will throw an error. Therefore, we are forced to keep
 * JsonFileAttributes to  bypass  "quards" and set manually file attributes.
 */
@Schema(description = "Specifies the attributes of a given file.")
public class JsonFileAttributes {

    @Schema(description = "NFSv4 Access control list.")
    private ACL _acl;

    @Schema(description = "File size in bytes.")
    private Long _size;

    @Schema(description = "File's attribute change timestamp, in unix-time.")
    private Long _ctime;

    @Schema(description = "File's creation timestamp, in unix-time.")
    private Long _creationTime;

    @Schema(description = "File's last access timestamp, in unix-time.")
    private Long _atime;

    @Schema(description = "File's last modification timestamp, in unix-time.")
    private Long _mtime;

    @Schema(description = "File's known checksums.")
    private Set<Checksum> _checksums;

    @Schema(description = "File owner's id.")
    private Integer _owner;

    @Schema(description = "Files group id.")
    private Integer _group;

    @Schema(description = "POSIX.1 file mode.")
    private Integer _mode;

    @Schema(description = "File's access latency.", allowableValues = {"ONLINE", "NEARLINE"})
    private AccessLatency _accessLatency;

    @Schema(description = "File's retention policy.",
          allowableValues = {"CUSTODIAL", "REPLICA", "OUTPUT"})
    private RetentionPolicy _retentionPolicy;

    @Schema(description = "File type.", allowableValues = {"REG", "DIR", "LINK", "SPECIAL"})
    private FileType _fileType;

    @Schema(description = "File's (disk) locations within dCache.")
    private Collection<String> _locations;

    @Schema(description = "Key value map of flags associated with the file.")
    private Map<String, String> _flags;

    @Schema(description = "Number of links.")
    private Integer _nlink;

    @Schema(description = "The PNFS-ID of the file (unique id).")
    private PnfsId _pnfsId;

    @Schema(description = "The storage info of the file.")
    private StorageInfo _storageInfo;

    @Schema(description = "List of storage (tape) uris.")
    private List<URI> _suris;

    @Schema(description = "Storage class of the file.")
    private String _storageClass;

    @Schema(description = "The HSM (hierarchical storage manager) storing the file.")
    private String _hsm;

    @Schema(description = "Cache class of the file.")
    private String _cacheClass;

    @Schema(description = "Name of the file.")
    private String fileName;

    @Schema(description = "MIME type of the file.")
    private String fileMimeType;

    @Schema(description = "Current parent directory of a file.")
    private String sourcePath;

    @Schema(description = "New directory of a file, where the file will be moved.")
    private String newPath;

    @Schema(description = "File path.")
    private String path;

    @Schema(description = "dCache file attributes for children, if this file is a directory.")
    private List<JsonFileAttributes> children;

    @Schema(description = "dCache file attributes of the file.")
    public FileAttributes attributes;

    @Schema(description = "Current file availability.",
          allowableValues = {"ONLINE", "NEARLINE", "ONLINE_AND_NEARLINE"})
    public FileLocality fileLocality;

    @Schema(description = "Current QoS for this file.")
    public String currentQos;

    @Schema(description = "The target QoS if the file is changing QoS.")
    public String targetQos;

    @Schema(description = "File's extended attributes.")
    private Map<String, String> xattr;

    @Schema(description = "File's labels.")
    private Set<String> labels;

    @Schema(description = "File's QoS policy.")
    private String qosPolicy;

    @Schema(description = "File's current QoS policy state index.")
    private Integer qosState;


    public ACL getAcl() {
        return _acl;
    }

    public void setAcl(ACL _acl) {
        this._acl = _acl;
    }

    public Integer getNlink() {
        return _nlink;
    }

    public void setNlink(Integer nlink) {
        _nlink = nlink;
    }

    public Long getSize() {
        return _size;
    }

    public void setSize(long _size) {
        this._size = _size;
    }

    public Long getCtime() {
        return _ctime;
    }

    public void setCtime(long _ctime) {
        this._ctime = _ctime;
    }

    public Long getCreationTime() {
        return _creationTime;
    }

    public void setCreationTime(long _creationTime) {
        this._creationTime = _creationTime;
    }

    public Long getAtime() {
        return _atime;
    }

    public void setAtime(long _atime) {
        this._atime = _atime;
    }

    public Long getMtime() {
        return _mtime;
    }

    public void setMtime(long _mtime) {
        this._mtime = _mtime;
    }

    public Set<Checksum> getChecksums() {
        return _checksums;
    }

    public void setChecksums(Set<Checksum> _checksums) {
        this._checksums = _checksums;
    }

    public Integer getOwner() {
        return _owner;
    }

    public void setOwner(int _owner) {
        this._owner = _owner;
    }

    public Integer getGroup() {
        return _group;
    }

    public void setGroup(int _group) {
        this._group = _group;
    }

    public Integer getMode() {
        return _mode;
    }

    public void setMode(int _mode) {
        this._mode = _mode;
    }

    public String getAccessLatency() {
        return _accessLatency == null ? null : _accessLatency.toString();
    }

    public void setAccessLatency(AccessLatency _accessLatency) {
        this._accessLatency = _accessLatency;
    }

    public String getRetentionPolicy() {
        return _retentionPolicy == null ? null : _retentionPolicy.toString();
    }

    public void setRetentionPolicy(RetentionPolicy _retentionPolicy) {
        this._retentionPolicy = _retentionPolicy;
    }

    public FileType getFileType() {
        return _fileType;
    }

    public void setFileType(FileType _fileType) {
        this._fileType = _fileType;
    }

    public Collection<String> getLocations() {
        return _locations;
    }

    public void setLocations(Collection<String> _locations) {
        this._locations = _locations;
    }

    public Map<String, String> getFlags() {
        return _flags;
    }

    public void setFlags(Map<String, String> _flags) {
        this._flags = _flags;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public void setPnfsId(PnfsId _pnfsId) {
        this._pnfsId = _pnfsId;
    }

    public StorageInfo getStorageInfo() {
        return _storageInfo;
    }

    public void setStorageInfo(StorageInfo _storageInfo) {
        this._storageInfo = _storageInfo;
    }

    public String getStorageClass() {
        return _storageClass;
    }

    public void setStorageClass(String _storageClass) {
        this._storageClass = _storageClass;
    }

    public List<URI> getSuris() {
        return _suris;
    }

    public void setSuris(List<URI> suris) {
        this._suris = suris;
    }

    public String getHsm() {
        return _hsm;
    }

    public void setHsm(String _hsm) {
        this._hsm = _hsm;
    }

    public String getCacheClass() {
        return _cacheClass;
    }

    public void setCacheClass(String _cacheClass) {
        this._cacheClass = _cacheClass;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileMimeType() {
        return fileMimeType;
    }

    public void setFileMimeType(String type) {
        fileMimeType = type;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getNewPath() {
        return newPath;
    }

    public void setNewPath(String newPath) {
        this.newPath = newPath;
    }

    public FileAttributes getAttributes() {
        return attributes;
    }

    public void setAttributes(FileAttributes attributes) {
        this.attributes = attributes;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public FileLocality getFileLocality() {
        return fileLocality;
    }

    public void setFileLocality(FileLocality fileLocality) {
        this.fileLocality = fileLocality;
    }

    public void setChildren(List<JsonFileAttributes> children) {
        this.children = children;
    }

    public List<JsonFileAttributes> getChildren() {
        return children;
    }

    public void setCurrentQos(String qos) {
        this.currentQos = qos;
    }

    public String getCurrentQos() {
        return currentQos;
    }

    public void setTargetQos(String qos) {
        targetQos = qos;
    }

    public String getTargetQos() {
        return targetQos;
    }

    public void setExtendedAttributes(Map<String, String> attributes) {
        xattr = attributes;
    }

    public Map<String, String> getExtendedAttributes() {
        return xattr;
    }

    public void setLabels(Set<String> labelnames) {
        if (labelnames == null) {
            return;
        }
        if (labels == null) {
            labels = new HashSet();
        }
        labels.addAll(labelnames);
    }

    public Set<String> getLabels() {
        return labels == null ? new HashSet() : labels;
    }

    public String getQosPolicy() {
        return qosPolicy;
    }

    public void setQosPolicy(String qosPolicy) {
        this.qosPolicy = qosPolicy;
    }

    public Integer getQosState() {
        return qosState;
    }

    public void setQosState(Integer qosState) {
        this.qosState = qosState;
    }
}