package diskCacheV111.admin ;

import java.util.Map ;
import java.util.List ;

/**
 * Author : Patrick Fuhrmann
 */
public interface UserMetaDataProvider {

    public Map getUserMetaData( String userName , String userRole , List attributes )
         throws Exception ;
}
