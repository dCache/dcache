package org.dcache.xrootd.protocol.messages;

import diskCacheV111.util.FsPath;

import org.dcache.xrootd.protocol.XrootdProtocol;

public class StatxRequest extends AbstractRequestMessage {

    public StatxRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_statx)
            throw new IllegalArgumentException("doesn't seem to be a kXR_statx message");
    }

    public String[] getPaths() {

        readFromHeader(false);

        String[] paths = new String(data).split("\n");
        for (int i = 0; i < paths.length; i++) {
            paths[i] =  new FsPath(paths[i]).toString();
        }
        return paths;
    }
}
