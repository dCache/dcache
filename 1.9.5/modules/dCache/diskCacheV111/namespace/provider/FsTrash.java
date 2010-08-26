/**
 * $Id$
 */
package diskCacheV111.namespace.provider;

import org.apache.log4j.Logger;

import java.io.File;

public class FsTrash implements Trash
{
    private final String _trash;
    private static final Logger _logger =  Logger.getLogger("logger.org.dcache.namespace." + FsTrash.class.getName());


    public FsTrash(String location)
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
        File inTrash = new File(_trash, pnfsid);
        return inTrash.exists();
    }

}
