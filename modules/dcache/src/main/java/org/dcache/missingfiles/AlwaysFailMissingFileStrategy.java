package org.dcache.missingfiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.util.FsPath;

/**
 * This class provides a MissingFileStrategy that recommends a door always
 * fails a missing file request.
 */
public class AlwaysFailMissingFileStrategy implements MissingFileStrategy
{
    private static final Logger _log =
            LoggerFactory.getLogger(AlwaysFailMissingFileStrategy.class);

    @Override
    public Action recommendedAction(Subject subject, FsPath dCachePath,
            String requestPath)
    {
        return Action.FAIL;
    }
}
