package org.dcache.xrootd.pool;

import java.io.IOException;

import org.dcache.xrootd.protocol.messages.ReadResponse;


/**
 * Encapsulates a read request. To avoid that we deplete memory space,
 * we only read as much data as we can write to the socket without
 * buffering. Hence a single read request may be broken into smaller
 * blocks internally. Each block is returned as an incomplete xrootd
 * response (with an "ok so far" response code).
 */
public interface Reader
{
    int getStreamID();
    ReadResponse read(int maxFrameSize)
        throws IOException;
}
