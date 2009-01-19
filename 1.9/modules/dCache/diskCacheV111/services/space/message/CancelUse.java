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
import diskCacheV111.util.PnfsId;

/**
 *
 * @author timur
 */
public class CancelUse extends Message{
    static final long serialVersionUID = 1530375623803317300L;
    private long spaceToken;
    private String pnfsName;
    private PnfsId pnfsId;
    /** Creates a new instance of Reserve */
    public CancelUse() {
    }
    
    public CancelUse(
            long spaceToken,
            String pnfsName,
            PnfsId pnfsId){
        this.spaceToken = spaceToken;
        this.pnfsName= pnfsName;
        this.pnfsId = pnfsId;
        setReplyRequired(true);
    }

    public long getSpaceToken() {
        return spaceToken;
    }

    public void setSpaceToken(long spaceToken) {
        this.spaceToken = spaceToken;
    }


    public String getPnfsName() {
        return pnfsName;
    }

    public void setPnfsName(String pnfsName) {
        this.pnfsName = pnfsName;
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public void setPnfsId(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
    }


}
