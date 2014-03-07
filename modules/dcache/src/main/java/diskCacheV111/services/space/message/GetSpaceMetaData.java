/*
 * Reserve.java
 *
 * Created on July 20, 2006, 8:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.services.space.message;

import diskCacheV111.services.space.Space;
import diskCacheV111.vehicles.Message;

/**
 *
 * @author timur
 */
public class GetSpaceMetaData extends Message{
    private static final long serialVersionUID = -7198244480807795469L;
    private final String[] spaceTokens;
    private Space[] spaces;

    public GetSpaceMetaData(String... spaceTokens) {
        this.spaceTokens = spaceTokens;
        setReplyRequired(true);
    }

    public String[] getSpaceTokens() {
        return spaceTokens;
    }

    public Space[] getSpaces() {
        return spaces;
    }

    public void setSpaces(Space[] spaces) {
        this.spaces = spaces;
    }


}
