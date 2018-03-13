package org.dcache.restful.providers;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.net.FileNameMap;
import java.net.URI;
import java.net.URLConnection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.acl.ACL;
import org.dcache.namespace.FileType;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

/**
 * This class is needed to mapping FileAttributes, when they are not defined.
 * The issue is that in the system not all attributes may be defined.
 * Attempts to read undefined attributes
 * will throw IllegalStateException( e.g. guard(CHECKSUM)).
 * So just returning FileAttributes in
 * FileResources.getFileAttributes will throw an error.
 * Therefore, we are forced to keep  JsonFileAttributes to  bypass  "quards"
 * and set manually file attributes.
 */
@ApiModel(description = "Specifies the attributes of a given file.")
public class JsonFileAttributes
{
    private static final FileNameMap MIME_TYPE_MAP =
            URLConnection.getFileNameMap();

    @ApiModelProperty("NFSv4 Access control list.")
    private ACL _acl;

    @ApiModelProperty("File size in bytes.")
    private Long _size;

    @ApiModelProperty("File's attribute change timestamp, in unix-time.")
    private Long _ctime;

    @ApiModelProperty("File's creation timestamp, in unix-time.")
    private Long _creationTime;

    @ApiModelProperty("File's last access timestamp, in unix-time.")
    private Long _atime;

    @ApiModelProperty("File's last modification timestamp, in unix-time.")
    private Long _mtime;

    @ApiModelProperty("File's known checksums.")
    private Set<Checksum> _checksums;

    @ApiModelProperty("File owner's id.")
    private Integer _owner;

    @ApiModelProperty("Files group id.")
    private Integer _group;

    @ApiModelProperty("POSIX.1 file mode.")
    private Integer _mode;

    @ApiModelProperty(value="File's access latency.", allowableValues = "ONLINE, NEARLINE")
    private AccessLatency _accessLatency;

    @ApiModelProperty(value="File's retention policy.", allowableValues = "CUSTODIAL, REPLICA, OUTPUT")
    private RetentionPolicy _retentionPolicy;

    @ApiModelProperty(value="File type.", allowableValues = "REG, DIR, LINK, SPECIAL")
    private FileType _fileType;

    @ApiModelProperty("File's (disk) locations within dCache.")
    private Collection<String> _locations;

    @ApiModelProperty("Key value map of flags associated with the file.")
    private Map<String, String> _flags;

    @ApiModelProperty("Number of links.")
    private Integer _nlink;

    @ApiModelProperty("The PNFS-ID of the file (unique id).")
    private PnfsId _pnfsId;

    @ApiModelProperty("The storage info of the file.")
    private StorageInfo _storageInfo;

    @ApiModelProperty("List of storage (tape) uris.")
    private List<URI> _suris;

    @ApiModelProperty("Storage class of the file.")
    private String _storageClass;

    @ApiModelProperty("The HSM (hierarchical storage manager) storing the file.")
    private String _hsm;

    @ApiModelProperty("Cache class of the file.")
    private String _cacheClass;

    @ApiModelProperty("Name of the file.")
    private String fileName;

    @ApiModelProperty("MIME type of the file.")
    private String fileMimeType;

    @ApiModelProperty("Current parent directory of a file.")
    private String sourcePath;

    @ApiModelProperty("New directory of a file, where the file will be moved.")
    private String newPath;

    @ApiModelProperty("File path.")
    private String path;

    @ApiModelProperty("dCache file attributes for children, if this file is a directory.")
    private List<JsonFileAttributes> children;

    @ApiModelProperty("dCache file attributes of the file.")
    public FileAttributes attributes;

    @ApiModelProperty(value="Current file availability.", allowableValues = "ONLINE, NEARLINE, ONLINE_AND_NEARLINE")
    public FileLocality fileLocality;

    @ApiModelProperty("Current QoS for this file.")
    public String currentQos;

    @ApiModelProperty("The target QoS if the file is changing QoS.")
    public String targetQos;

    public ACL getAcl() {
        return _acl;
    }

    public void setAcl(ACL _acl) {
        this._acl = _acl;
    }

    public Integer getNlink() { return _nlink; }

    public void setNlink(Integer nlink) { _nlink = nlink; }

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

    public  Integer getGroup() {
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
        return _accessLatency == null?  null : _accessLatency.toString();
    }

    public void setAccessLatency(AccessLatency _accessLatency) {
        this._accessLatency = _accessLatency;
    }

    public String getRetentionPolicy() {
        return _retentionPolicy == null? null : _retentionPolicy.toString();
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

    public String getFileMimeType()
    {
        return fileMimeType;
    }

    public void setFileMimeType(String fileName)
    {
        switch (this._fileType) {
            case DIR:
                this.fileMimeType = "application/vnd.dcache.folder";
                break;
            case LINK:
                this.fileMimeType = "application/vnd.dcache.link";
                break;
            case SPECIAL:
                this.fileMimeType = "application/vnd.dcache.special";
                break;
            default:
                if (MIME_TYPE_MAP.getContentTypeFor(fileName) == null) {
                    this.fileMimeType = "application/octet-stream";
                } else {
                    this.fileMimeType = MIME_TYPE_MAP.getContentTypeFor(fileName);
                }
        }
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

}