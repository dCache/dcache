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
    private String pinId;
    private long newLifetime;
    
    //private static final long serialVersionUID;
    
    /** Creates a new instance of PinManagerPinMessage */
    public PinManagerExtendLifetimeMessage(String pnfsId, String pinId, long newLifetime) {
        super(pnfsId);
        this.pinId = pinId;
        this.newLifetime = newLifetime;
    }
    
    public PinManagerExtendLifetimeMessage(PnfsId pnfsId, String pinId, long newLifetime) {
        super(pnfsId);
        this.pinId = pinId;
        this.newLifetime = newLifetime;
    }
    
   
    /** Getter for property pinId.
     * @return Value of property pinId.
     *
     */
    public java.lang.String getPinId() {
        return pinId;
    }
    
    /** Setter for property pinId.
     * @param pinId New value of property pinId.
     *
     */
    public void setPinId(java.lang.String pinId) {
        this.pinId = pinId;
    }
    
    public String toString() {
        return "PinManagerUnpinMessage["+getPnfsId()+
                (pinId==null?"":(","+pinId))+
                "]";
    }

    public long getNewLifetime() {
        return newLifetime;
    }

    public void setNewLifetime(long newLifetime) {
        this.newLifetime = newLifetime;
    }
        
}
