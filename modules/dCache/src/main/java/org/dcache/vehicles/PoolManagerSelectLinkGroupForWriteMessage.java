package org.dcache.vehicles;

import java.util.List;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.ProtocolInfo;

/**
 * Message to project a set of link groups to the link groups capable
 * of accepting a given file.
 *
 * The message basically contains two pieces of information: A
 * description of a file that is about to be written (e.g. PnfsId,
 * StorageInfo and ProtocolInfo) and optionally a set of link groups.
 *
 * The reply to the message should hold the subset of the set of link
 * groups that would actually be able to select a write pool for the
 * given file.
 *
 * If no link groups where provided in the request message, i.e.
 * getLinkGroups returns null, then PoolManager must consider all link
 * groups.
 */
public class PoolManagerSelectLinkGroupForWriteMessage extends Message
{
    private PnfsId _pnfsId;
    private StorageInfo _storageInfo;
    private ProtocolInfo _protocolInfo;
    private long _fileSize;
    private String _pnfsPath;
    private List<String> _linkGroups;

    public PoolManagerSelectLinkGroupForWriteMessage(PnfsId pnfsId,
                                                     StorageInfo storageInfo,
                                                     ProtocolInfo protocolInfo,
                                                     long fileSize)
    {
        _pnfsId = pnfsId;
        _storageInfo = storageInfo;
        _protocolInfo = protocolInfo;
        _fileSize = fileSize;
    }

    public long getFileSize()
    {
        return _fileSize;
    }

    public void setFileSize(long fileSize)
    {
        _fileSize = fileSize;
    }

    public StorageInfo getStorageInfo()
    {
        return _storageInfo;
    }

    public void setStorageInfo(StorageInfo storageInfo)
    {
        _storageInfo = storageInfo;
    }

    public ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    public void setProtocolInfo(ProtocolInfo protocolInfo)
    {
        _protocolInfo = protocolInfo;
    }

    public String getPnfsPath()
    {
        return _pnfsPath;
    }

    public void setPnfsPath(String pnfsPath)
    {
        _pnfsPath = pnfsPath;
    }

    public List<String> getLinkGroups()
    {
        return _linkGroups;
    }

    public void setLinkGroups(List<String> linkGroups)
    {
        _linkGroups = linkGroups;
    }
}
