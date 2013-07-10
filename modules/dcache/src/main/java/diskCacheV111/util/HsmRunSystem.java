package diskCacheV111.util;

import java.io.IOException;

/**
 * Utility class for invoking an HSM integration script.
 */
public class HsmRunSystem extends RunSystem
{
    public HsmRunSystem(String exec, int maxLines, long timeout)
    {
        super(exec, maxLines, timeout);
    }

    public String execute() throws IOException, CacheException
    {
        go();
        int returnCode = getExitValue();
        switch (returnCode) {
        case 0:
            break;
        case 71:
            throw new CacheException(CacheException.HSM_DELAY_ERROR,
                    "HSM script failed (script reported 71: " + getErrorString() + ")");
        case 143:
            throw new TimeoutCacheException(
                    "HSM script was killed (script reported 143: " + getErrorString() + ")");
        default:
            throw new CacheException(returnCode,
                    "HSM script failed (script reported: " + returnCode + ": " + getErrorString());
        }
        return getOutputString();
    }
}
