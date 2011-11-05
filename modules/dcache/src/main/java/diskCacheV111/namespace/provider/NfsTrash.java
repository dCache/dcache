package diskCacheV111.namespace.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import static java.text.MessageFormat.*;

/**
 * $Id$
 * User: podstvkv
 * Date: Feb 12, 2009
 * Time: 6:50:15 PM
 */
public class NfsTrash implements Trash
{
    private final String _trash;
    private static final Logger _logger =  LoggerFactory.getLogger("logger.org.dcache.namespace");


    public NfsTrash(String location)
    {
        if (location == null)
            throw new IllegalArgumentException("Bad trash location");
        File trashLocation = new File(location);
        if ((!trashLocation.exists()) || (!trashLocation.isDirectory())) {
            throw new IllegalArgumentException("Directory '" + location + "' does not exist");
        } else {
            _trash = location;
            if (_logger.isDebugEnabled()) {
                _logger.debug("Trash location set: " + _trash);
            }
        }
    }

    public boolean isFound(String pnfsid) {
        // Check if such entry exists in the trash and return the result
        File inTrash = new File(_trash, format(".(intrash)({0})", pnfsid));
        return inTrash.exists();
    }
}
