package org.dcache.vehicles;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsMessage;

import org.dcache.util.ChecksumType;

/**
 * Request that a checksum value of the specified type is removed.
 */
public class PnfsRemoveChecksumMessage extends PnfsMessage
{
    private final ChecksumType _type;

    public PnfsRemoveChecksumMessage(PnfsId pnfsId, ChecksumType type)
    {
        super(pnfsId);
        _type = type;
    }

    public ChecksumType getType()
    {
        return _type;
    }
}
