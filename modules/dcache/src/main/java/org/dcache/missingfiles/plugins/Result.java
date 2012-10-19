package org.dcache.missingfiles.plugins;

/**
 * The result of a plugin processing a missing-file notification.  This is
 * in the form of suggested behaviour of the plugin.
 */
public enum Result
{
    /**
     *  The door should fail the request.
     */
    FAIL,

    /**
     *  The door should retry the request.  This reply indicates that the
     *  plugin has fetched the missing file from some external source.
     */
    RETRY,

    /**
     *  The plugin does not suggest what the door should do.
     */
    DEFER;
}
