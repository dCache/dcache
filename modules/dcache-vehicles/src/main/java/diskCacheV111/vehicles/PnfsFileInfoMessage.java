package diskCacheV111.vehicles;

import java.util.Objects;

import diskCacheV111.util.PnfsId;

public abstract class PnfsFileInfoMessage extends InfoMessage
{
    private PnfsId _pnfsId;
    private String _path = "Unknown";
    private long _fileSize;
    private StorageInfo _storageInfo;

    private static final long serialVersionUID = -7761016173336078097L;

    public PnfsFileInfoMessage(String messageType,
                               String cellType,
                               String cellName,
                               PnfsId pnfsId)
    {
        super(messageType, cellType, cellName);
        _pnfsId = pnfsId;
    }

    public String getFileInfo()
    {
        return "[" + _pnfsId + "," + _fileSize + "]" + " " + "[" + _path + "] " +
               (_storageInfo == null ?
                "<unknown>" :
                (_storageInfo.getStorageClass() + "@" + _storageInfo.getHsm()));
    }

    public void setFileSize(long fileSize)
    {
        _fileSize = fileSize;
    }

    public long getFileSize()
    {
        return _fileSize;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public void setPnfsId(PnfsId pnfsId)
    {
        _pnfsId = pnfsId;
    }

    public void setStorageInfo(StorageInfo storageInfo)
    {
        _storageInfo = storageInfo;
    }

    public StorageInfo getStorageInfo()
    {
        return _storageInfo;
    }

    public String getBillingPath()
    {
        return _path;
    }

    public void setBillingPath(String path)
    {
        _path = Objects.toString(path, "Unknown");
    }
}
