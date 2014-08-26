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
public class ExtendLifetime extends Message{
    private static final long serialVersionUID = -8269395310293892754L;
    private long spaceToken;
    private long newLifetime;
    /** Creates a new instance of Reserve */
    public ExtendLifetime() {
    }

    public ExtendLifetime(
            long spaceToken,
            long newLifetime){
        this.spaceToken = spaceToken;
        this.newLifetime = newLifetime;
        setReplyRequired(true);
    }

    public long getSpaceToken() {
        return spaceToken;
    }

    public void setSpaceToken(long spaceToken) {
        this.spaceToken = spaceToken;
    }

    public long getNewLifetime() {
        return newLifetime;
    }

    public void setNewLifetime(long newLifetime) {
        this.newLifetime = newLifetime;
    }


}
