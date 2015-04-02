package org.dcache.acl.enums;

/**
 * Mandatory Attributes.
 * based: Network File System (NFS) version 4 Protocol, RFC3530
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public enum FileAttribute {

    /**
     * Mandatory Attributes
     */
    FATTR4_SUPPORTED_ATTRS    (0),
    FATTR4_TYPE               (1),
    FATTR4_FH_EXPIRE_TYPE     (2),
    FATTR4_CHANGE             (3),
    FATTR4_SIZE               (4),
    FATTR4_LINK_SUPPORT       (5),
    FATTR4_SYMLINK_SUPPORT    (6),
    FATTR4_NAMED_ATTR         (7),
    FATTR4_FSID               (8),
    FATTR4_UNIQUE_HANDLES     (9),
    FATTR4_LEASE_TIME         (10),
    FATTR4_RDATTR_ERROR       (11),
    FATTR4_FILEHANDLE         (19),

    /**
     * Recommended Attributes
     */
    FATTR4_ACL                (12),
    FATTR4_ACLSUPPORT         (13),
    FATTR4_ARCHIVE            (14),
    FATTR4_CANSETTIME         (15),

    FATTR4_CASE_INSENSITIVE   (16),
    FATTR4_CASE_PRESERVING    (17),
    FATTR4_CHOWN_RESTRICTED   (18),
    FATTR4_FILEID             (20),
    FATTR4_FILES_AVAIL        (21),
    FATTR4_FILES_FREE         (22),
    FATTR4_FILES_TOTAL        (23),
    FATTR4_FS_LOCATIONS       (24),
    FATTR4_HIDDEN             (25),
    FATTR4_HOMOGENEOUS        (26),
    FATTR4_MAXFILESIZE        (27),
    FATTR4_MAXLINK            (28),
    FATTR4_MAXNAME            (29),
    FATTR4_MAXREAD            (30),
    FATTR4_MAXWRITE           (31),
    FATTR4_MIMETYPE           (32),
    FATTR4_MODE               (33),
    FATTR4_NO_TRUNC           (34),
    FATTR4_NUMLINKS           (35),
    FATTR4_OWNER              (36),
    FATTR4_OWNER_GROUP        (37),
    FATTR4_QUOTA_AVAIL_HARD   (38),
    FATTR4_QUOTA_AVAIL_SOFT   (39),
    FATTR4_QUOTA_USED         (40),
    FATTR4_RAWDEV             (41),
    FATTR4_SPACE_AVAIL        (42),
    FATTR4_SPACE_FREE         (43),
    FATTR4_SPACE_TOTAL        (44),
    FATTR4_SPACE_USED         (45),
    FATTR4_SYSTEM             (46),
    FATTR4_TIME_ACCESS        (47),
    FATTR4_TIME_ACCESS_SET    (48),
    FATTR4_TIME_BACKUP        (49),
    FATTR4_TIME_CREATE        (50),
    FATTR4_TIME_DELTA         (51),
    FATTR4_TIME_METADATA      (52),
    FATTR4_TIME_MODIFY        (53),
    FATTR4_TIME_MODIFY_SET    (54),
    FATTR4_MOUNTED_ON_FILEID  (55),
    FATTR4_DIR_NOTIF_DELAY    (56),
    FATTR4_DIRENT_NOTIF_DELAY (57),
    FATTR4_DACL               (58),
    FATTR4_SACL               (59),
    FATTR4_CHANGE_POLICY      (60),
    FATTR4_FS_STATUS          (61),
    FATTR4_FS_LAYOUT_TYPE     (62),
    FATTR4_LAYOUT_HINT        (63),
    FATTR4_LAYOUT_TYPE        (64),
    FATTR4_LAYOUT_BLKSIZE     (65),
    FATTR4_LAYOUT_ALIGNMENT   (66),
    FATTR4_FS_LOCATIONS_INFO  (67),
    FATTR4_MDSTHRESHOLD       (68),
    FATTR4_RETENTION_GET      (69),
    FATTR4_RETENTION_SET      (70),
    FATTR4_RETENTEVT_GET      (71),
    FATTR4_RETENTEVT_SET      (72),
    FATTR4_RETENTION_HOLD     (73),
    FATTR4_MODE_SET_MASKED    (74),
    FATTR4_SUPPATTR_EXCLCREAT (75),
    FATTR4_FS_CHARSET_CAP     (76);

    private final int _value;

    private FileAttribute(int value) {
        _value = value;
    }

    public int getValue() {
        return _value;
    }

    public boolean equals(int value) {
        return _value == value;
    }

    public boolean matches(int flags) {
        return (_value & flags) == _value;
    }

    /**
     * @param attribute
     * @return FileAttribute
     */
    public static FileAttribute valueOf(int attribute) throws IllegalArgumentException {
        for (FileAttribute attr : FileAttribute.values()) {
            if (attr._value == attribute) {
                return attr;
            }
        }

        throw new IllegalArgumentException("Illegal argument (value of file attribute): " + attribute);
    }

    /**
     * @param attributes
     *            Attributes bit mask
     * @return Return string representation of attributes bit mask
     */
    public static String asString(int attributes) {
        StringBuilder sb = new StringBuilder();
        for (FileAttribute attr : FileAttribute.values()) {
            if ( attr.matches(attributes) ) {
                if ( sb.length() != 0 ) {
                    sb.append(" | ");
                }
                sb.append(attr);
            }
        }
        return sb.toString();
    }
}
