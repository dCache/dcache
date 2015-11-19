package diskCacheV111.util;

import org.dcache.srm.v2_2.TFileLocality;

public enum FileLocality
{
    ONLINE             (TFileLocality.ONLINE, true),
    NEARLINE           (TFileLocality.NEARLINE, false),
    ONLINE_AND_NEARLINE(TFileLocality.ONLINE_AND_NEARLINE, true),
    LOST               (TFileLocality.LOST, false),
    NONE               (TFileLocality.NONE, false),
    UNAVAILABLE        (TFileLocality.UNAVAILABLE,false);

    private final TFileLocality _locality;
    private final boolean _isCached;

    FileLocality(TFileLocality locality, boolean cached) {
        _locality = locality;
        _isCached = cached;
    }

    public TFileLocality toTFileLocality() {
        return _locality;
    }

    public boolean isCached() {
        return _isCached;
    }

}
