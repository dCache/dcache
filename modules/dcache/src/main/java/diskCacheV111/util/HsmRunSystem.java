package diskCacheV111.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.Severity;
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

    public HsmRunSystem(String storageName, String exec, int maxLines, long timeout)
    {
        super(exec, maxLines, timeout);
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
        } catch (CacheException e) {
            LOGGER.error(AlarmMarkerFactory.getMarker(Severity.HIGH,
                                                      "HSM_SCRIPT_FAILURE",
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

        int index = error.indexOf(PNFSID_TAG);
        if (index >= 0) {
            index += PNFSID_TAG.length();
            ids.append(error.substring(index, error.indexOf("&", index)));
        }

        index = error.indexOf(BFID_TAG);
        if (index >= 0) {
            index += BFID_TAG.length();
            ids.append(error.substring(index, error.indexOf("&", index)));
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
