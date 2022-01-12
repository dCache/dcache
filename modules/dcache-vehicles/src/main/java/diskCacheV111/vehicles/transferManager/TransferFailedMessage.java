/*
 * TransferFailedMessage.java
 *
 * Created on November 23, 2004, 2:16 PM
 */

package diskCacheV111.vehicles.transferManager;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author timur
 */
public class TransferFailedMessage extends TransferManagerMessage {

    private static final long serialVersionUID = 8704471271670009325L;

    private final List<Duration> performance;

    /**
     * Creates a new instance of TransferFailedMessage
     */
    public TransferFailedMessage(TransferManagerMessage original, int errorCode,
          Serializable errorObject, List<Duration> performance) {
        super(original);
        setFailed(errorCode, errorObject);
        this.performance = requireNonNull(performance);
    }

    public List<Duration> getPerformance() {
        return performance;
    }
}
