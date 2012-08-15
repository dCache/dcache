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
public class TransferCompleteMessage extends TransferManagerMessage{

    private static final long serialVersionUID = -2819277237066589628L;

    /** Creates a new instance of TransferFailedMessage */
    public TransferCompleteMessage(TransferManagerMessage original) {
        super(original);
    }

}
