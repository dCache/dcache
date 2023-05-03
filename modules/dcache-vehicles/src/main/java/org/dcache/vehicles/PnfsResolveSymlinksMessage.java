/*
 * $Id: PnfsRenameMessage.java,v 1.2 2005-02-21 15:49:33 tigran Exp $
 */

package org.dcache.vehicles;

import diskCacheV111.vehicles.PnfsMessage;

public class PnfsResolveSymlinksMessage extends PnfsMessage {

    private static final long serialVersionUID = 5670471419391898275L;

    private final String prefix;

    private String resolvedPrefix;

    private String resolvedPath;

    public PnfsResolveSymlinksMessage(String path) {
        this(path, null);
    }

    public PnfsResolveSymlinksMessage(String path, String prefix) {
        setPnfsPath(path);
        this.prefix = prefix;
        setReplyRequired(true);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getResolvedPath() {
        return resolvedPath;
    }

    public void setResolvedPath(String resolvedPath) {
        this.resolvedPath = resolvedPath;
        setSucceeded();
    }

    public String getResolvedPrefix() {
        return resolvedPrefix;
    }

    public void setResolvedPrefix(String resolvedPrefix) {
        this.resolvedPrefix = resolvedPrefix;
    }
}
