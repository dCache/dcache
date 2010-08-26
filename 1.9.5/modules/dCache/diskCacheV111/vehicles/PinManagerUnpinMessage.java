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
    private Long srmRequestId;
    
    private static final long serialVersionUID = -2435476735204095804L;
    
    /** Creates a new instance of PinManagerPinMessage */
    public PinManagerUnpinMessage(String pnfsId) {
        super(pnfsId);
    }
    
    public PinManagerUnpinMessage(PnfsId pnfsId) {
        super(pnfsId);
    }
    
    /** Creates a new instance of PinManagerPinMessage */
    public PinManagerUnpinMessage(String pnfsId, String pinRequestId) {
        super(pnfsId);
        setPinRequestId(pinRequestId);
    }
    
    public PinManagerUnpinMessage(String pnfsId, Long srmRequestId) {
        super(pnfsId);
        this.srmRequestId = srmRequestId;
    }
     
    public PinManagerUnpinMessage(PnfsId pnfsId, String pinRequestId) {
        super(pnfsId);
        setPinRequestId(pinRequestId);
    }
    
    public PinManagerUnpinMessage(PnfsId pnfsId, Long srmRequestId) {
        super(pnfsId);
        this.srmRequestId = srmRequestId;
    }
   
    @Override
    public String toString() {
        return "PinManagerUnpinMessage["+getPnfsId()+
                (getPinRequestId()==null?"":(","+getPinRequestId()))+
                "]";
    }

    public Long getSrmRequestId() {
        return srmRequestId;
    }

    public void setSrmRequestId(Long srmRequestId) {
        this.srmRequestId = srmRequestId;
    }
        
}
