package org.dcache.vehicles;

import java.util.List;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.namespace.FileAttribute;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
    private static final long serialVersionUID = -5329660627613167395L;
    private PnfsId _pnfsId;
    private FileAttributes _fileAttributes;
    private ProtocolInfo _protocolInfo;
    private long _fileSize;
    private String _pnfsPath;
    private List<String> _linkGroups;

    public PoolManagerSelectLinkGroupForWriteMessage(PnfsId pnfsId,
                                                     FileAttributes fileAttributes,
                                                     ProtocolInfo protocolInfo,
                                                     long fileSize)
    {
        _pnfsId = checkNotNull(pnfsId);
        _fileAttributes = checkNotNull(fileAttributes);
        _protocolInfo = checkNotNull(protocolInfo);
        checkArgument(fileAttributes.isDefined(FileAttribute.STORAGEINFO));
        _fileSize = fileSize;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public long getFileSize()
    {
        return _fileSize;
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    public List<String> getLinkGroups()
    {
        return _linkGroups;
    }

    public void setLinkGroups(List<String> linkGroups)
    {
        _linkGroups = linkGroups;
    }

    @Override
    public String getDiagnosticContext()
    {
        return super.getDiagnosticContext() + " " + getPnfsId();
    }

}
