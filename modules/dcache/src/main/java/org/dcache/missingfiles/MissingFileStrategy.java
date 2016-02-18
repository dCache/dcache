package org.dcache.missingfiles;

import javax.security.auth.Subject;

import diskCacheV111.util.FsPath;

/**
 * A MissingFileStrategy can decide how a door should react when a user has
 * requested a file that does not exist.
 */
public interface MissingFileStrategy
{
    /**
     * Discover what action a door should take when a file is missing.
     * @param subject the user that make this request
     * @param requestPath the file's path, as requested by the user
     * @param dCachePath the path to the missing file within dCache
     * @return recommended behaviour for the door
     */
    Action recommendedAction(Subject subject, FsPath dCachePath, String requestPath);
}
