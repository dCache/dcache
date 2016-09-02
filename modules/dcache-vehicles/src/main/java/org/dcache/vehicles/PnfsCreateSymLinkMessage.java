package org.dcache.vehicles;

import diskCacheV111.vehicles.PnfsCreateEntryMessage;

/**
 * A message to create a symbolic link
 */
public class PnfsCreateSymLinkMessage extends PnfsCreateEntryMessage
{
    private static final long serialVersionUID = -7174229288877933004L;

    private final String _destination;

    public PnfsCreateSymLinkMessage(String path, String dest, FileAttributes attributes)
    {
        super(path, attributes);
        _destination = dest;
    }

    public String getDestination()
    {
        return _destination;
    }
}
