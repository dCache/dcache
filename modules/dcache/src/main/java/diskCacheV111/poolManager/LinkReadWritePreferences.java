package diskCacheV111.poolManager;

/**
 * Simple Datacontainer - Clumps together all sort of read- writepreferences a
 * link in dCache has
 * @author jans
 */
public class LinkReadWritePreferences {

    private final int _readPref;
    private final int _writePref;
    private final int _restorePref;
    private final int _p2pPref;

    public LinkReadWritePreferences(int readPref, int writePref, int restorePref, int p2pPref) {
        _readPref = readPref;
        _writePref = writePref;
        _restorePref = restorePref;
        _p2pPref = p2pPref;
    }

    public int getRestorePref() {
        return _restorePref;
    }

    public int getP2pPref() {
        return _p2pPref;
    }

    public int getReadPref() {
        return _readPref;
    }

    public int getWritePref() {
        return _writePref;
    }
}
