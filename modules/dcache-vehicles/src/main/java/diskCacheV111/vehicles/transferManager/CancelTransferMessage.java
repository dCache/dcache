package diskCacheV111.vehicles.transferManager;

import diskCacheV111.vehicles.Message;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class CancelTransferMessage extends Message {

    private static final long serialVersionUID = -4445578968029755510L;
    long callerUniqueId;
    private String explanation;

    public CancelTransferMessage(long id, long callerUniqueId) {
        super();
        setId(id);
        this.callerUniqueId = callerUniqueId;
        setReplyRequired(false);
    }

    public long getCallerUniqueId() {
        return callerUniqueId;
    }

    public void setExplanation(String explanation) {
        this.explanation = explanation;
    }

    public String getExplanation() {
        return explanation;
    }
}



