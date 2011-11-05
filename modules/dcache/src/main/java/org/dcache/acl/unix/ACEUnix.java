package org.dcache.acl.unix;

/**
 * Unix Access Control Entry (ACE).
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class ACEUnix {

    private static final String SEPARATOR = ":";

    /**
     * The ACE tags (combination of values from AceTags enumeration)
     */
    private int _tags;

    /**
     * The access mask (combination of values from AccessMask enumeration)
     */
    private int _accessMsk;

    /**
     * @param tags
     *            ACE tags
     */
    public ACEUnix(int tags) {
        _tags = tags;
    }

    /**
     * @param tags
     *            ACE tags
     * @param accessMsk
     *            Access mask
     */
    public ACEUnix(int tags, int accessMsk) {
        _tags = tags;
        _accessMsk = accessMsk;
    }

    public int getAccessMsk() {
        return _accessMsk;
    }

    public void setAccessMsk(int accessMsk) {
        _accessMsk = accessMsk;
    }

    public int getTags() {
        return _tags;
    }

    public void setTags(int tags) {
        _tags = tags;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(AceTag.toString(_tags)).append(SEPARATOR).append(AMUnix.toString(_accessMsk));
        return sb.toString();
    }

}
