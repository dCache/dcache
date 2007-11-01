/*
 * Reserve.java
 *
 * Created on July 20, 2006, 8:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space.message;

import diskCacheV111.vehicles.Message;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.services.space.Space;

/**
 *
 * @author timur
 */
public class GetSpaceTokens extends Message{
    private long[] spaceTokens;
    private String description;
    private String voRole;
    private String voGroup;
    /** Creates a new instance of Reserve */
    public GetSpaceTokens(String voGroup, String voRole,String description) {
        this.voGroup = voGroup;
        this.voRole = voRole;
        this.description = description;
        setReplyRequired(true);
    }
    
    public long[] getSpaceTokens() {
        return spaceTokens;
    }

    public void setSpaceToken(long spaceTokens[]) {
        this.spaceTokens = spaceTokens;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getVoRole() {
        return voRole;
    }

    public void setVoRole(String voRole) {
        this.voRole = voRole;
    }

    public String getVoGroup() {
        return voGroup;
    }

    public void setVoGroup(String voGroup) {
        this.voGroup = voGroup;
    }

}
