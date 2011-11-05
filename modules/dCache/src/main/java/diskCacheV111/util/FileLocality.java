package diskCacheV111.util;

import org.dcache.srm.v2_2.TFileLocality;

public enum FileLocality
{
    ONLINE             (TFileLocality.ONLINE),
    NEARLINE           (TFileLocality.NEARLINE),
    ONLINE_AND_NEARLINE(TFileLocality.ONLINE_AND_NEARLINE),
    LOST               (TFileLocality.LOST),
    NONE               (TFileLocality.NONE),
    UNAVAILABLE        (TFileLocality.UNAVAILABLE);

    private final TFileLocality _locality;

    private FileLocality(TFileLocality locality) {
        _locality = locality;
    }

    public TFileLocality toTFileLocality()
    {
        return _locality;
    }
}
