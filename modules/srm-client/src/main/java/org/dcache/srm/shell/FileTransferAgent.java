package org.dcache.srm.shell;

import org.apache.axis.types.URI;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.Map;

/**
 * A concrete class that implements FileTransferAgent organises the
 * transfer of a file between some URL and some local file.
 */
public interface FileTransferAgent extends AutoCloseable
{
    /**
     * Called precisely once, before download, upload or
     * getSupportedProtocols.
     */
    void start();

    /**
     * A name for this transport.  The value should be lower-case and unique
     * in the set of transports.  Return null if this FileTransferAgent should
     * not be exposed to the end user.
     */
    String getTransportName();

    /**
     * The options that may be configured and their current values.
     */
    @Nonnull
    Map<String,String> getOptions();

    /**
     * Alter an option.
     */
    void setOption(String key, String value);

    /**
     * Download a file to a locally-attached storage medium
     * (e.g., harddisk) from some remote location.
     * @return the facade for this transfer or null if the URI cannot be handled
     * by this agent.
     */
    FileTransfer download(URI source, File destination);

    /**
     * Upload a file from a locally-attached storage medium
     * (e.g., harddisk) to some remote location.
     * @return the facade for this transfer or null if the URI cannot be handled
     * by this agent.
     */
    FileTransfer upload(File source, URI destination);

    /**
     * Provide a list of transfer protocols with corresponding priority.
     * The higher the integer value, the greater the priority with which the
     * protocol should be selected.
     */
    Map<String,Integer> getSupportedProtocols();
}
