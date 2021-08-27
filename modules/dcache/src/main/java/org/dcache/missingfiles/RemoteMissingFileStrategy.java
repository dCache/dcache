package org.dcache.missingfiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;

/**
 * This class provides a recommendation on how a door should handle a missing
 * file.  It provides this recommendation by contacting a 'missing-files'
 * service.
 */
public class RemoteMissingFileStrategy implements MissingFileStrategy
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RemoteMissingFileStrategy.class);

    private CellStub _stub;

    public void setMissingFilesCellStub(CellStub stub)
    {
        _stub = stub;
    }


    @Override
    public Action recommendedAction(Subject subject, FsPath dCachePath, String requestPath)
    {
        MissingFileMessage msg
                = new MissingFileMessage(requestPath, dCachePath.toString());
        msg.setSubject(subject);

        MissingFileMessage reply;

        try {
            reply = _stub.sendAndWait(msg);
            return reply.getAction();
        } catch (NoRouteToCellException | CacheException e) {
            LOGGER.error(e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.info("interrupted while waiting for advise from missing-files service");
        }

        return Action.FAIL;
    }
}
