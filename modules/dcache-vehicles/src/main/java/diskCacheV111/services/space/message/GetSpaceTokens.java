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

/**
 *
 * @author timur
 */
public class GetSpaceTokens extends Message{
    private static final long serialVersionUID = -2482510383290374236L;
    private long[] spaceTokens;
    private String description;
    /** Creates a new instance of Reserve */
    public GetSpaceTokens(String description) {
        this.description = description;
        setReplyRequired(true);
    }

    public long[] getSpaceTokens() {
        return spaceTokens;
    }

    public void setSpaceToken(long[] spaceTokens) {
        this.spaceTokens = spaceTokens;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
