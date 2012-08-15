/*
 * TransferFailedMessage.java
 *
 * Created on November 23, 2004, 2:16 PM
 */

package diskCacheV111.vehicles.transferManager;

/**
 *
 * @author  timur
 */
public class TransferFailedMessage extends TransferManagerMessage{

    private static final long serialVersionUID = 8704471271670009325L;

    /** Creates a new instance of TransferFailedMessage */
    public TransferFailedMessage(TransferManagerMessage original,int errorCode,Object errorObject) {
        super(original);
        setFailed(errorCode, errorObject);
    }

}
