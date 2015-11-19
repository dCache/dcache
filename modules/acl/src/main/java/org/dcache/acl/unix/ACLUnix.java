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

    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    public static final String SEPARATOR = ":";

    /**
     * True if resource type is Directory, otherwise false
     */
    private final boolean _isDir;

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
    public ACLUnix(boolean isDir) {
        _isDir= isDir;
        if (_isDir) {
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
    public ACLUnix(boolean isDir, int[] accessMasks, int[] defaultMasks) {
        this(isDir);
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
     * @return True if resource is a directory, otherwise false
     */
    public boolean isDir() {
        return _isDir;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("Unix ACL:");
        sb.append(LINE_SEPARATOR);

        sb.append("isDir = ").append(_isDir);
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
}
