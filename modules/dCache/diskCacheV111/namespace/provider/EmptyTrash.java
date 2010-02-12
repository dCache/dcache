package diskCacheV111.namespace.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.namespace.provider.Trash;

/**
 * $Id$
 */
public class EmptyTrash implements Trash
{
    private final String _trash;
    private static final Logger _logger =  LoggerFactory.getLogger("logger.org.dcache.namespace." + EmptyTrash.class.getName());


    public EmptyTrash(String location)
    {
        _trash = location;
        if (_logger.isDebugEnabled()) {
            _logger.debug("Trash location set: " + _trash);
        }
    }


    public boolean isFound(String pnfsid) {
        return false;
    }
    
}
