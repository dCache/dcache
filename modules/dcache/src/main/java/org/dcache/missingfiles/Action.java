package org.dcache.missingfiles;

/**
 * Recommended behaviour for a door provided by the missing file service.
 */
public enum Action
{
    /**
     * The door should retry the request.  This is sent when some
     * external script has populated the missing file.
     */
    RETRY,

    /**
     * The door should fail the request.
     */
    FAIL
}
