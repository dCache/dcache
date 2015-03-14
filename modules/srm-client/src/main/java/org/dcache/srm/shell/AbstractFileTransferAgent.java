package org.dcache.srm.shell;

import org.apache.axis.types.URI;

import java.io.File;
import java.util.Collections;
import java.util.Map;

/**
 * A simple FileTransferAgent that doesn't support anything.
 */
public abstract class AbstractFileTransferAgent implements FileTransferAgent
{
    @Override
    public void start()
    {
        // Nothing needed.
    }

    /**
     * The options that may be configured and their current values.
     */
    @Override
    public Map<String,String> getOptions()
    {
        return Collections.emptyMap();
    }

    /**
     * Alter an option.
     */
    @Override
    public void setOption(String key, String value)
    {
        throw new IllegalArgumentException("No such option \"" + key + "\"");
    }


    @Override
    public FileTransfer download(URI source, File destination)
    {
        return null; // URI schema not supported.
    }

    @Override
    public FileTransfer upload(File source, URI destination)
    {
        return null; // URI schema not supported.
    }

    @Override
    public Map<String, Integer> getSupportedProtocols()
    {
        return Collections.emptyMap();
    }

    @Override
    public void close() throws Exception
    {
        // Nothing needed.
    }
}
