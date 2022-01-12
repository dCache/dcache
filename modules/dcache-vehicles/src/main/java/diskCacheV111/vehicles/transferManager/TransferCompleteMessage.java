/*
 * TransferFailedMessage.java
 *
 * Created on November 23, 2004, 2:16 PM
 */

package diskCacheV111.vehicles.transferManager;

import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author timur
 */
public class TransferCompleteMessage extends TransferManagerMessage {

    private static final long serialVersionUID = -2819277237066589628L;

    private final List<Duration> performance;

    /**
     * Creates a new instance of TransferFailedMessage
     */
    public TransferCompleteMessage(TransferManagerMessage original, List<Duration> performance) {
        super(original);
        this.performance = requireNonNull(performance);
    }

    public List<Duration> getPerformance() {
        return performance;
    }
}
