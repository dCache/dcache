package org.dcache.namespace;

/**
 *
 * File attributes supported by dCache
 *
 * @since 1.9.4
 */
public enum FileAttribute {

    ACCESS_LATENCY,
    ACCESS_TIME,
    ACL,
    CHECKSUM,
    CHANGE_TIME,   // Not supported in 2.6, so be careful not to send this to pools
    CREATION_TIME,
    FLAGS,
    HSM,
    LOCATIONS,
    MODE,
    MODIFICATION_TIME,
    OWNER,
    OWNER_GROUP,
    PERMISSION,
    RETENTION_POLICY,
    SIZE,
    STORAGECLASS,
    STORAGEINFO,
    TYPE,
    SIMPLE_TYPE,
    PNFSID;
}
