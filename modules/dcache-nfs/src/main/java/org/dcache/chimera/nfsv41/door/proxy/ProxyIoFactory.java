package org.dcache.chimera.nfsv41.door.proxy;

import java.io.IOException;
import java.util.function.Consumer;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;

/**
 * Classes that implement this interface are responsible for creating
 * and managing IO proxies.
 */
public interface ProxyIoFactory {

    /**
     * Returns a proxy that is bound to the stateid. An existing proxy
     * may be returned or new proxy is created if no suitable proxy exists for
     * this stateid.
     */
    ProxyIoAdapter getOrCreateProxy(Inode inode, stateid4 stateid,
            CompoundContext context, boolean isWrite) throws IOException;

    /**
     * Create a new proxy that is bound to the given stateid.  This
     * happens irrespective of whether a proxy already exists for this stateid.
     */
    ProxyIoAdapter createIoAdapter(Inode inode, stateid4 stateid,
            CompoundContext context, boolean isWrite) throws IOException;

    /**
     * Close the proxy adaptor.  After this method returns, subsequent
     * calls to {@link #getOrCreateProxy} will return a new proxy.
     */
    void shutdownAdapter(stateid4 stateid);

    /**
     * Performs the given action for active {@link ProxyIoAdapter}.
     */
    void forEach(Consumer<ProxyIoAdapter> action);

    /**
     * Close all active proxies and free up any additional resources.
     * After calling this method, the behavior of all other methods is not
     * guaranteed.
     */
    void shutdown();
}
