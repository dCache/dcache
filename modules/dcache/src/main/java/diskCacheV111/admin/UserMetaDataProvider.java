package diskCacheV111.admin ;

import java.util.List;
import java.util.Map;

/**
 * Author : Patrick Fuhrmann
 */
public interface UserMetaDataProvider {

    Map<String,String> getUserMetaData(String userName, String userRole, List<String> attributes)
         throws Exception ;
}
