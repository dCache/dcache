/*
 * PinManagerPinMessage.java
 *
 * Created on April 28, 2004, 1:51 PM
 */

package diskCacheV111.vehicles;
import diskCacheV111.util.PnfsId;

/**
 *
 * @author  timur
 */
public class PinManagerExtendLifetimeMessage extends PinManagerMessage {
    private static final long serialVersionUID = 6259613705437421481L;
    private long newLifetime;
    
    //private static final long serialVersionUID;
    
    /** Creates a new instance of PinManagerPinMessage */
    public PinManagerExtendLifetimeMessage(String pnfsId, String pinRequestId, long newLifetime) {
        super(pnfsId);
        setPinRequestId(pinRequestId);
        this.newLifetime = newLifetime;
    }
    
    public PinManagerExtendLifetimeMessage(PnfsId pnfsId, String pinRequestId, long newLifetime) {
        super(pnfsId);
        setPinRequestId(pinRequestId);
        this.newLifetime = newLifetime;
    }
    
   
    
    public String toString() {
        return "PinManagerUnpinMessage["+getPnfsId()+
                (getPinRequestId()==null?"":(","+getPinRequestId()))+
                "]";
    }

    public long getNewLifetime() {
        return newLifetime;
    }

    public void setNewLifetime(long newLifetime) {
        this.newLifetime = newLifetime;
    }
        
}
