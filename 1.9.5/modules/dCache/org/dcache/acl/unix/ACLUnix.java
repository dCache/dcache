package org.dcache.acl.unix;

/**
 * Unix Access Control List (ACL).
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class ACLUnix {

    public static final int NUM_ACES = 3;

    public static final int OWNER_INDEX = 0;

    public static final int GROUP_OWNER_INDEX = 1;

    public static final int OTHER_INDEX = 2;

    private final static String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    public static final String SEPARATOR = ":";

    /**
     * ACL Identifier
     */
    private String _rsId;

    /**
     * True if resource type is Directory, otherwise false
     */
    private boolean _isDir;

    ACEUnix accessAceOwner = new ACEUnix(AceTag.USER_OBJ.getValue(), 1);

    ACEUnix accessAceOwnerGroup = new ACEUnix(AceTag.GROUP_OBJ.getValue());

    ACEUnix accessAceOther = new ACEUnix(AceTag.OTHER_OBJ.getValue());

    ACEUnix defaultAceOwner;

    ACEUnix defaultAceOwnerGroup;

    ACEUnix defaultAceOther;

    /**
     * @param rsId
     *            Resource Identifier
     * @param isDir
     *            True if resource is a directory
     */
    public ACLUnix(String rsId, boolean isDir) {
        _rsId = rsId;
        if ( _isDir = isDir ) {
            defaultAceOwner = new ACEUnix(AceTag.DEFAULT.getValue() | AceTag.USER_OBJ.getValue());
            defaultAceOwnerGroup = new ACEUnix(AceTag.DEFAULT.getValue() | AceTag.GROUP_OBJ.getValue());
            defaultAceOther = new ACEUnix(AceTag.DEFAULT.getValue() | AceTag.OTHER_OBJ.getValue());
        }
    }

    /**
     * @param rsId
     *            Resource Identifier
     * @param isDir
     *            True if resource is a directory
     * @param accessMasks
     *            Array of access masks
     * @param defaultMasks
     *            Array of default masks
     */
    public ACLUnix(String rsId, boolean isDir, int[] accessMasks, int[] defaultMasks) {
        this(rsId, isDir);
        accessAceOwner.setAccessMsk(accessMasks[OWNER_INDEX]);
        accessAceOwnerGroup.setAccessMsk(accessMasks[GROUP_OWNER_INDEX]);
        accessAceOther.setAccessMsk(accessMasks[OTHER_INDEX]);
        if ( _isDir ) {
            defaultAceOwner.setAccessMsk(defaultMasks[OWNER_INDEX]);
            defaultAceOwnerGroup.setAccessMsk(defaultMasks[GROUP_OWNER_INDEX]);
            defaultAceOther.setAccessMsk(defaultMasks[OTHER_INDEX]);
        }
    }

    /**
     * @return Returns ACL resource identifier
     */
    public String getRsId() {
        return _rsId;
    }

    /**
     * @return True if resource is a directory, otherwise false
     */
    public boolean isDir() {
        return _isDir;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("Unix ACL:");
        sb.append(LINE_SEPARATOR);

        sb.append("rsId = ").append(_rsId).append(", isDir = ").append(_isDir);
        sb.append(LINE_SEPARATOR);

        sb.append(accessAceOwner.toString());
        sb.append(", ").append(accessAceOwnerGroup.toString());
        sb.append(", ").append(accessAceOther.toString());
        sb.append(LINE_SEPARATOR);

        if ( _isDir ) {
            sb.append(defaultAceOwner.toString());
            sb.append(", ").append(defaultAceOwnerGroup.toString());
            sb.append(", ").append(defaultAceOther.toString());
        }
        return sb.toString();
    }

    public String toFullString() {
        return toString(true);
    }

    public String toUnixString() {
        return toString(false);
    }

    private String toString(boolean detailed) {
        StringBuilder sb = new StringBuilder();
        if (detailed)
            sb.append(_rsId).append(SEPARATOR);

        final char dir_abbreviation = 'd';
        if ( _isDir )
            sb.append(dir_abbreviation);
        else
            sb.append("-");

        sb.append(AMUnix.toString(accessAceOwner.getAccessMsk()));
        sb.append(AMUnix.toString(accessAceOwnerGroup.getAccessMsk()));
        sb.append(AMUnix.toString(accessAceOther.getAccessMsk()));

        if ( detailed && _isDir && (defaultAceOwner.getAccessMsk() != 0 || defaultAceOwnerGroup.getAccessMsk() != 0 || defaultAceOther.getAccessMsk() != 0) ) {
            sb.append(SEPARATOR).append(dir_abbreviation);
            sb.append(AMUnix.toString(defaultAceOwner.getAccessMsk()));
            sb.append(AMUnix.toString(defaultAceOwnerGroup.getAccessMsk()));
            sb.append(AMUnix.toString(defaultAceOther.getAccessMsk()));
        }
        return sb.toString();
    }

}
