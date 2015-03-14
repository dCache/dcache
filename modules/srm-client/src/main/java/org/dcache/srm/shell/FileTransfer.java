package org.dcache.srm.shell;

import com.google.common.util.concurrent.ListenableFuture;


/**
 * A class that implements FileTransfer acts as a facade for a specific
 * file transfer.
 */
public interface FileTransfer extends ListenableFuture<Void>
{
    String getStatus();
}
