package org.dcache.namespace;

/**
 * File attributes supported by dCache
 *
 * @since 1.9.4
 */
public enum FileAttribute {

    ACCESS_LATENCY,
    ACCESS_TIME,
    ACL,
    CACHECLASS,
    CHECKSUM,
    CHANGE_TIME,
    CREATION_TIME,
    FLAGS,
    HSM,
    LOCATIONS,
    MODE,
    MODIFICATION_TIME,
    OWNER,
    OWNER_GROUP,
    RETENTION_POLICY,
    SIZE,
    STORAGECLASS,
    STORAGEINFO,
    TYPE,
    SIMPLE_TYPE,
    PNFSID,

    /**
     * @since 3.0
     */
    NLINK,
    /**
     * @since 6.2
     */
    XATTR,

    /**
     * @since 7.2
     */
    LABELS,

    /**
     * @since 9.2
     */
    QOS_POLICY,
    QOS_STATE

}
