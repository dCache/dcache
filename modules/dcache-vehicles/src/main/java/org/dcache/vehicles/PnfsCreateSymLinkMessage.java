package org.dcache.vehicles;

import diskCacheV111.vehicles.PnfsCreateEntryMessage;

/**
 * A message to create a symbolic link
 */
public class PnfsCreateSymLinkMessage extends PnfsCreateEntryMessage
{
    private static final long serialVersionUID = -7174229288877933004L;

    private final String _destination;

    public PnfsCreateSymLinkMessage(String path, String dest, int uid, int gid) {
        // the value of mode is irrelevant
        super(path, uid, gid, -1);
        _destination = dest;
    }

    public String getDestination() {
        return _destination;
    }
}
