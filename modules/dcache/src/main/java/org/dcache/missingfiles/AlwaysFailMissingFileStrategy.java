package org.dcache.missingfiles;

import diskCacheV111.util.FsPath;
import javax.security.auth.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a MissingFileStrategy that recommends a door always fails a missing file
 * request.
 */
public class AlwaysFailMissingFileStrategy implements MissingFileStrategy {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(AlwaysFailMissingFileStrategy.class);

    @Override
    public Action recommendedAction(Subject subject, FsPath dCachePath,
          String requestPath) {
        return Action.FAIL;
    }
}
