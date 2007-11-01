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
public class PinManagerUnpinMessage extends PinManagerMessage {
    private String pinId;
    
    private static final long serialVersionUID = -2435476735204095804L;
    
    /** Creates a new instance of PinManagerPinMessage */
    public PinManagerUnpinMessage(String pnfsId, String pinId) {
        super(pnfsId);
        this.pinId = pinId;
    }
    
    public PinManagerUnpinMessage(PnfsId pnfsId, String pinId) {
        super(pnfsId);
        this.pinId = pinId;
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
        
}
