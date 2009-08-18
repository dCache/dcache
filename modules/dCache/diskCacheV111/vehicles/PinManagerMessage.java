//$Id: PinManagerMessage.java,v 1.6 2005-08-15 18:19:43 timur Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.PnfsId ;
import org.dcache.auth.AuthorizationRecord;

//Base class for messages to PinManager


public class PinManagerMessage extends Message {
    private StorageInfo  storageInfo;
    private PnfsId pnfsId = null;
    private AuthorizationRecord authorizationRecord;
    private String pinRequestId;

    private static final long serialVersionUID = 8985723922810987125L;
    
    public PinManagerMessage(String pnfsId){
        this.pnfsId = new PnfsId(pnfsId) ;
        setReplyRequired(true);
    }

    public PinManagerMessage(PnfsId pnfsId){
	this.pnfsId = pnfsId ;
        setReplyRequired(true);
    }

    public PinManagerMessage()
    {
        setReplyRequired(true);
    }
    

    public PnfsId getPnfsId(){
	return pnfsId;
    }
    
    public void setPnfsId(String pnfsId){
	this.pnfsId = new PnfsId(pnfsId);
    }
    public void setPnfsId(PnfsId pnfsId){
	this.pnfsId = pnfsId ;
    }

    @Override
    public String toString(){ 
        return pnfsId==null?
               "NULL":("PnfsId="+pnfsId.toString()) ; 
    }
    
    /** Getter for property storageInfo.
     * @return Value of property storageInfo.
     *
     */
    public StorageInfo getStorageInfo() {
        return storageInfo;
    }
    
    /** Setter for property storageInfo.
     * @param storageInfo New value of property storageInfo.
     *
     */
    public void setStorageInfo(StorageInfo storageInfo) {
        this.storageInfo = storageInfo;
    }

    public AuthorizationRecord getAuthorizationRecord() {
        return authorizationRecord;
    }

    public void setAuthorizationRecord(AuthorizationRecord authorizationRecord) {
        this.authorizationRecord = authorizationRecord;
    }
    /** Getter for property pinId.
     * @return Value of property pinId.
     *
     */
    public String getPinRequestId() {
        return pinRequestId;
    }

    /** Setter for property pinId.
     * @param pinId New value of property pinId.
     *
     */
    public void setPinRequestId(String pinId) {
        this.pinRequestId = pinId;
    }
    
}


    
