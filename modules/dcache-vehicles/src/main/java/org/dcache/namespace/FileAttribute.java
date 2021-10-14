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
    CACHECLASS,    // Not supported before 2.12, so be careful not to send this to pools
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
    NLINK,   // Be careful not to send this to pools before next golden release
    /**
     * @since 6.2
     */
    XATTR, // Be careful not to send this to pools before next golden release (7.2)

    /**
     * @since 7.2
     */
    LABELS


}
