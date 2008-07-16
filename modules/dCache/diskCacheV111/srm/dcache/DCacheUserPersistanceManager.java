/*
 * DCacheUserPersistanceManager.java
 *
 * Created on July 15, 2008, 11:55 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.srm.dcache;

import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMUserPersistenceManager;
import java.util.Map;
import java.util.HashMap;


/**
 *
 * @author timur
 */
public class DCacheUserPersistanceManager implements SRMUserPersistenceManager {
    Map<Long, SRMUser> userMap = new HashMap<Long, SRMUser> ();
    
    /** Creates a new instance of DCacheUserPersistanceManager */
    public DCacheUserPersistanceManager() {
    }
    
    public long persist(SRMUser srmUser) {
        userMap.put(srmUser.getId(), srmUser);
        return srmUser.getId();
    }
    
    public SRMUser retrieve(long persistenceId) {
        return userMap.get(persistenceId);
        
    }
    
}
