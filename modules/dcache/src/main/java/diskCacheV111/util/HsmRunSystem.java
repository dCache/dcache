package diskCacheV111.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.NetworkUtils;

/**
 * Utility class for invoking an HSM integration script.
 */
public class HsmRunSystem extends RunSystem
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HsmRunSystem.class);
    private static final String PNFSID_TAG = "pnfsid_file=";
    private static final String BFID_TAG = "bfid=";

    private final String storageName;

    public HsmRunSystem(String storageName, int maxLines, long timeout, String ... exec)
    {
        super(maxLines, timeout, exec);
        this.storageName = storageName;
    }

    public String execute() throws IOException, CacheException
    {
        go();
        int returnCode = getExitValue();
        try {
            switch (returnCode) {
            case 0:
                break;
            case 71:
                throw new CacheException(CacheException.HSM_DELAY_ERROR,
                                         "HSM script failed (script reported 71: "
                                         + getErrorString() + ")");
            case 72:
                throw new InProgressCacheException(72, "HSM script requested retry (script reported 72: "
                                                       + getErrorString() + ")");
            case 143:
                throw new TimeoutCacheException(
                        "HSM script was killed (script reported 143: "
                        + getErrorString() + ")");
            default:
                throw new CacheException(returnCode,
                                         "HSM script failed (script reported: "
                                         + returnCode + ": "
                                         + getErrorString());
            }
        } catch (InProgressCacheException e) {
            throw e;
        } catch (CacheException e) {
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.HSM_SCRIPT_FAILURE,
                                                      NetworkUtils.getCanonicalHostName(),
                                                      storageName,
                                                      extractPossibleEnstoreIds(getErrorString())),
                                    e.getMessage());
            throw e;
        }
        return getOutputString();
    }

    private String extractPossibleEnstoreIds(String error) {
        StringBuilder ids = new StringBuilder();

        /*
         *  FIXME  this method should not really be in this class,
         *  but in an enstore-specific script utility.
         *  The check for the end tag is a provisional work-around.
         *
         *  NOTE:  I am not sure whether the mere presence of the ampersand
         *  here denotes an enstore error string or not, so I've left
         *  each check as self-contained for the moment.
         */
        int start = error.indexOf(PNFSID_TAG);
        if (start >= 0) {
            start += PNFSID_TAG.length();
            int end = error.indexOf('&', start);
            if (end >= 0) {
                ids.append(PNFSID_TAG)
                   .append(error.substring(start, end));
            }
        }

        start = error.indexOf(BFID_TAG);
        if (start >= 0) {
            start += BFID_TAG.length();
            int end = error.indexOf('&', start);
            if (end > 0) {
                ids.append(BFID_TAG)
                   .append(error.substring(start, end));
            }
        }

        /*
         * if the message does not contain data based on f_enstore2uri
         * then just enforce a match on the error string.
         */
        if (ids.length() == 0) {
            ids.append(error);
        }

        return ids.toString();
    }
}
