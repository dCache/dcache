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
public class PinManagerPinMessage extends PinManagerMessage {
    private long lifetime;
    private long requestId;
    private String readPoolName;
    private String clientHost;
    
    
    private static final long serialVersionUID = 5292501618388506009L;
    
    /** Creates a new instance of PinManagerPinMessage */
    public PinManagerPinMessage(String pnfsId, 
        String clientHost, 
        long lifetime, 
        long requestId) {
        super(pnfsId);
        this.lifetime = lifetime;
        this.requestId = requestId;
        this.clientHost = clientHost;
    }
    
    public PinManagerPinMessage(PnfsId pnfsId,
        String clientHost,
        long lifetime, 
        long requestId) {
        super(pnfsId);
        this.lifetime = lifetime;
        this.requestId = requestId;
        this.clientHost = clientHost;
   }
    
    /** Getter for property lifetime.
     * @return Value of property lifetime.
     *
     */
    public long getLifetime() {
        return lifetime;
    }    
    
    /** Setter for property lifetime.
     * @param lifetime New value of property lifetime.
     *
     */
    public void setLifetime(long lifetime) {
        this.lifetime = lifetime;
    }
    
    /** Getter for property readPoolName.
     * @return Value of property readPoolName.
     *
     */
    public java.lang.String getReadPoolName() {
        return readPoolName;
    }
    
    /** Setter for property readPoolName.
     * @param readPoolName New value of property readPoolName.
     *
     */
    public void setReadPoolName(java.lang.String readPoolName) {
        this.readPoolName = readPoolName;
    }

    @Override
    public String toString() {
        return "PinManagerPinMessage["+getPnfsId()+
                (getPinRequestId()==null?"":(","+getPinRequestId()))+
                "]";
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public String getClientHost() {
        return clientHost;
    }
    
}
