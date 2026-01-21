package diskCacheV111.util;

public enum FileLocality {
    ONLINE(true),
    NEARLINE(false),
    ONLINE_AND_NEARLINE(true),
    LOST(false),
    NONE(false),
    UNAVAILABLE(false);

    private final boolean _isCached;

    FileLocality(boolean cached) {
        _isCached = cached;
    }


    public boolean isCached() {
        return _isCached;
    }

}
